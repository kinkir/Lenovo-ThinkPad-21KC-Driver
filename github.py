#!/usr/bin/env python3
from typing import Any, Optional, TypeAlias
import datetime
import io
import logging
import os
import re
import requests
import subprocess
import urllib3

logger = logging.getLogger(__name__)

GITHUB_REPOSITORY = os.environ['GITHUB_REPOSITORY']
GITHUB_TOKEN = os.environ['GITHUB_TOKEN']

session = requests.Session()
session.cookies._policy.set_ok = lambda cookie, request: False
github_headers = {
    'Accept': 'application/vnd.github.v3+json',
    'Authorization': f'token {GITHUB_TOKEN}',
}

ReleaseDict: TypeAlias = dict[str, Any]
ReleaseAssetDict: TypeAlias = dict[str, Any]

def release_ensure(tag_name: str, name: str, timestamp: Optional[datetime.datetime] = None) -> ReleaseDict:
    release = release_get_by_tag(tag_name)
    if not release:
        release = release_create(tag_name, name, timestamp)
    return release

def release_get_by_tag(tag_name: str) -> ReleaseDict:
    rsp = session.get(
        url=f'https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/tags/{tag_name}',
        headers=github_headers,
        allow_redirects=False,
    )
    if rsp.status_code == 200:
        return rsp.json()
    assert rsp.status_code == 404, f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'

def release_create(tag_name: str, name: str, timestamp: Optional[datetime.datetime] = None) -> ReleaseDict:
    if timestamp is None:
        timestamp = datetime.datetime.now()
    date = timestamp.astimezone(datetime.UTC).isoformat() + 'Z'
    subprocess.run(f'''set -eux
COMMIT=$(git commit-tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904 </dev/null)
git tag -f {tag_name} $COMMIT
git push -f -u origin refs/tags/{tag_name}
''',
        env=os.environ | dict(
            GIT_AUTHOR_NAME='Bot',
            GIT_AUTHOR_EMAIL='bot@example.com',
            GIT_AUTHOR_DATE=date,
            GIT_COMMITTER_NAME='Bot',
            GIT_COMMITTER_EMAIL='bot@example.com',
            GIT_COMMITTER_DATE=date,
        ),
        shell=True,
        check=True,
    )
    rsp = session.post(
        url=f'https://api.github.com/repos/{GITHUB_REPOSITORY}/releases',
        headers=github_headers,
        json={
            'tag_name': tag_name,
            'name': name,
        },
        allow_redirects=False,
    )
    assert rsp.status_code == 201, f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'
    return rsp.json()

def release_asset_name_sanitize(name: str) -> str:
    if name in ['.', '..']:
        raise ValueError('invalid name')
    dotfile = name[0] == '.'
    name = re.sub(r'[^-+@_0-9A-Za-z]+', '.', name)
    if '.' not in name:
        return name
    else:
        name = name.strip('.')
        name = re.sub(r'\.+', '.', name)
        filename = 'default'
        extension = 'default'
        if dotfile:
            if name:
                extension = name
        else:
            parts = name.split('.', 1)
            extension = parts.pop()
            if parts:
                filename = parts[0]
        return f'{filename}.{extension}'

def release_asset_list(release: ReleaseDict) -> list[ReleaseAssetDict]:
    assets = []
    url = release['assets_url'] + '?per_page=100'
    while 1:
        rsp = session.get(
            url=url,
            headers=github_headers,
            allow_redirects=False,
        )
        assert rsp.status_code == 200, f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'
        assets.extend(rsp.json())
        if 'next' not in rsp.links:
            break
        url = rsp.links['next']['url']
    return assets

def release_asset_get(release: ReleaseDict, name: str) -> Optional[ReleaseAssetDict]:
    asset_name = release_asset_name_sanitize(name)
    for asset in release_asset_list(release):
        if asset['name'] == asset_name:
            return asset
    return None

def release_asset_delete(release: ReleaseDict, name: str) -> None:
    asset_name = release_asset_name_sanitize(name)
    for asset in release_asset_list(release):
        if asset['name'] == asset_name:
            break
    else:
        raise ValueError(f'asset {name} not found')
    logger.info('deleting %s', asset['name'])
    rsp = session.delete(
        url=asset['url'],
        headers=github_headers,
        allow_redirects=False,
    )
    assert rsp.status_code == 204, f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'

def release_asset_download(asset: ReleaseAssetDict, stream: bool = False) -> requests.Response:
    logger.info('downloading %s', asset['name'])
    while True:
        rsp = session.get(
            url=asset['url'],
            headers=github_headers | {'Accept': 'application/octet-stream'},
            allow_redirects=True,
            stream=stream,
        )
        if rsp.status_code == 403 and 'Signature not valid in the specified time frame' in rsp.text:
            ## retry
            continue
        assert rsp.status_code == 200, f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'
        return rsp

ASSET_SIZE_LIMIT = 2*1024*1024*1024-1 # 2GiB - 1B

def release_asset_upload(release: ReleaseDict, name: str, fileobj: io.BufferedIOBase, label: Optional[str] = None) -> ReleaseAssetDict:
    logger.info('uploading %s', name)

    ## obtain and check file size
    fileobj.seek(0, io.SEEK_END)
    size = fileobj.tell()
    assert 0 < size <= ASSET_SIZE_LIMIT

    ## upload with retry
    upload_url = release['upload_url'].split('{', 1)[0]
    for retry in range(3):
        fileobj.seek(0)
        try:
            params = {'name': name}
            if label:
                params['label'] = label
            rsp = session.post(upload_url,
                params=params,
                headers=github_headers | {'Content-Type': 'application/octet-stream'},
                data=fileobj,
                allow_redirects=False,
            )
            if rsp.status_code == 201:
                return rsp.json()
            logger.error(f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}')
            if rsp.status_code == 422:
                ## assume {"resource":"ReleaseAsset","code":"already_exists","field":"name"}
                try:
                    release_asset_delete(release, name)
                except Exception:
                    pass
            else:
                assert rsp.status_code in [500, 502, 504], f'HTTP {rsp.status_code} {rsp.reason}\n{rsp.text}'
        except (requests.ConnectionError, urllib3.exceptions.HTTPError):
            logger.exception('connection error')
            ## somehow, existed assets may also cause ConnectionError
            try:
                release_asset_delete(release, name)
            except Exception:
                pass
        logger.error('upload failed, retry')
    else:
        raise RuntimeError('upload aborted')

class LimitedReader:
    def __init__(self, f: io.BufferedIOBase, offset: int, limit: int) -> None:
        assert offset >= 0
        assert limit >= 0
        self._file = f
        self._offset = offset
        self._limit = limit
        self._pos = 0

    def seekable(self) -> bool:
        return True

    def seek(self, offset, whence=io.SEEK_SET) -> int:
        match whence:
            case io.SEEK_SET:
                pass
            case io.SEEK_CUR:
                offset += self._pos
            case io.SEEK_END:
                offset += self._limit
            case _:
                raise ValueError('unsupported whence')
        offset = max(offset, 0)
        offset = min(offset, self._limit)
        self._pos = offset
        return offset

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> Optional[bytes]:
        real_pos = self._offset + self._pos
        if self._file.tell() != real_pos:
            self._file.seek(real_pos)
        if size == -1:
            size = self._limit - self._pos
        data = self._file.read(size)
        if data:
            self._pos += len(data)
        return data

    def writable(self) -> bool:
        return False

DEFAULT_CHUNK_SIZE = 2046*1024*1024 # 2GiB - 2MiB

def release_asset_upload_with_autosplit(release: ReleaseDict, name: str, fileobj: io.BufferedIOBase, label: Optional[str] = None) -> ReleaseAssetDict | list[ReleaseAssetDict]:
    logger.info('uploading %s', name)

    ## obtain and check file size
    fileobj.seek(0, io.SEEK_END)
    size = fileobj.tell()
    assert size > 0

    if size <= ASSET_SIZE_LIMIT:
        return release_asset_upload(
            release=release,
            name=name,
            fileobj=fileobj,
            label=label,
        )
    else:
        num_chunks = (size - 1) // DEFAULT_CHUNK_SIZE + 1
        result = []
        for chunk_idx in range(num_chunks):
            logger.info('part %d/%d', chunk_idx+1, num_chunks)
            if chunk_idx == num_chunks - 1:
                chunk_size = (size - 1) % DEFAULT_CHUNK_SIZE + 1
            else:
                chunk_size = DEFAULT_CHUNK_SIZE
            result.append(release_asset_upload(
                release=release,
                name=f'{name}.{chunk_idx+1}',
                fileobj=LimitedReader(fileobj, chunk_idx * DEFAULT_CHUNK_SIZE, chunk_size),
                label=f'{label} (part {chunk_idx+1}/{num_chunks})' if label else None,
            ))
        return result
