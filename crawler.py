#!/usr/bin/env python3
"""
Lenovo ThinkPad driver crawler and preserver.
Downloads driver updates and preserves them in GitHub releases.

Checksum Verification Policy:
    - Strict verification for executables (.exe), firmware, and other binary files
    - Lenient verification for documentation files (.txt, .html)

    Rationale: Lenovo frequently updates readme/documentation files when adding
    support for new machine types, but fails to update the corresponding SHA256
    checksums in the metadata XML files. ThinkVantage System Update (TVSU) only
    verifies checksums for executable components, not documentation. We follow
    the same approach to avoid spurious failures while maintaining security for
    actual driver/firmware files.
"""
import hashlib
import io
import logging
import os
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import requests
import yaml

import github

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG_FILE = 'config.yaml'
METADATA_ASSET_NAME = '_metadata.xml'
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Global error tracking
has_errors = False


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(CONFIG_FILE)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {CONFIG_FILE} not found")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if 'machine_type' not in config or 'os' not in config:
        raise ValueError("Configuration must contain 'machine_type' and 'os'")

    return config


def download_with_retry(url: str, max_retries: int = MAX_RETRIES) -> Optional[bytes]:
    """Download content from URL with retry logic."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.content
        except (requests.RequestException, Exception) as e:
            logger.warning(f"Download failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                return None
    return None


def compute_sha256(content: bytes) -> str:
    """Compute SHA256 hash of content."""
    return hashlib.sha256(content).hexdigest().upper()


def verify_checksum(content: bytes, expected: str, name: str, strict: bool = True) -> bool:
    """
    Verify content checksum matches expected value.

    Args:
        content: File content to verify
        expected: Expected SHA256 checksum
        name: File name for logging
        strict: If True, checksum mismatch is an error. If False, only a warning.

    Returns:
        True if checksum matches or if non-strict mode, False otherwise.

    Note on non-strict mode:
        Lenovo frequently updates readme files (.txt, .html) for packages without
        updating the corresponding checksums in metadata XML files. This appears to
        happen when they add support for new machine types and revise documentation.
        ThinkVantage System Update (TVSU) only verifies checksums for executable files,
        not documentation files, so we follow the same approach to avoid false failures.
    """
    actual = compute_sha256(content)
    if actual != expected.upper():
        if strict:
            logger.error(f"Checksum mismatch for {name}: expected {expected}, got {actual}")
            return False
        else:
            logger.warning(f"Checksum mismatch for {name}: expected {expected}, got {actual}")
            logger.warning(f"Ignoring checksum mismatch for documentation file {name}")
            return True
    return True


def parse_catalog(xml_content: bytes) -> list[dict]:
    """Parse catalog XML and return list of packages."""
    root = ET.fromstring(xml_content)
    packages = []

    for pkg_elem in root.findall('package'):
        location = pkg_elem.find('location')
        checksum_elem = pkg_elem.find('checksum')

        if location is not None and checksum_elem is not None:
            packages.append({
                'location': location.text,
                'checksum': checksum_elem.text,
            })

    logger.info(f"Found {len(packages)} packages in catalog")
    return packages


def parse_package_metadata(xml_content: bytes) -> Optional[dict]:
    """Parse package metadata XML and extract relevant information."""
    try:
        root = ET.fromstring(xml_content)

        # Extract package ID, version, name
        package_id = root.get('id')
        package_version = root.get('version')

        if not package_id:
            logger.error("Package ID not found in metadata")
            return None

        # Extract title
        title_elem = root.find(".//Title/Desc[@id='EN']")
        if title_elem is None:
            title_elem = root.find(".//Title/Desc")
        title = title_elem.text if title_elem is not None else package_id

        # Extract release date
        release_date_elem = root.find('ReleaseDate')
        release_date = release_date_elem.text if release_date_elem is not None else None

        # Extract all files
        files = extract_all_files(root)

        return {
            'id': package_id,
            'version': package_version,
            'title': title,
            'release_date': release_date,
            'files': files,
        }
    except Exception as e:
        logger.error(f"Failed to parse package metadata: {e}")
        return None


def extract_all_files(package_root: ET.Element) -> list[dict]:
    """Extract all files from package metadata XML."""
    files = []
    files_elem = package_root.find('Files')

    if files_elem is None:
        return files

    # Iterate all file type elements (Installer, Readme, External, etc.)
    for file_type_elem in files_elem:
        # Handle both simple and localized file structures
        for file_elem in file_type_elem.findall('.//File'):
            try:
                name_elem = file_elem.find('Name')
                crc_elem = file_elem.find('CRC')
                size_elem = file_elem.find('Size')

                if name_elem is not None and crc_elem is not None:
                    name = name_elem.text
                    crc = crc_elem.text
                    size = int(size_elem.text) if size_elem is not None else 0

                    files.append({
                        'name': name,
                        'crc': crc,
                        'size': size,
                        'url': f'https://download.lenovo.com/pccbbs/mobiles/{name}',
                    })
            except Exception as e:
                logger.warning(f"Failed to extract file info: {e}")
                continue

    logger.info(f"Extracted {len(files)} files from metadata")
    return files


def get_stored_metadata(release: dict) -> Optional[bytes]:
    """Get stored metadata XML from release assets."""
    try:
        asset = github.release_asset_get(release, METADATA_ASSET_NAME)
        if asset is None:
            logger.info("No stored metadata found in release")
            return None

        logger.info("Downloading stored metadata from release")
        response = github.release_asset_download(asset)
        return response.content
    except Exception as e:
        logger.warning(f"Failed to get stored metadata: {e}")
        return None


def compare_files(stored_files: list[dict], current_files: list[dict]) -> list[dict]:
    """Compare file lists and return files that need to be downloaded."""
    stored_file_map = {f['name']: f for f in stored_files}
    files_to_download = []

    for current_file in current_files:
        name = current_file['name']
        if name not in stored_file_map:
            logger.info(f"New file detected: {name}")
            files_to_download.append(current_file)
        elif stored_file_map[name]['crc'] != current_file['crc']:
            logger.info(f"File changed (checksum differs): {name}")
            files_to_download.append(current_file)
        else:
            logger.debug(f"File unchanged: {name}")

    return files_to_download


def download_and_upload_file(release: dict, file_info: dict) -> bool:
    """Download file from Lenovo, verify checksum, and upload to release."""
    name = file_info['name']
    url = file_info['url']
    expected_crc = file_info['crc']

    logger.info(f"Processing file: {name}")

    # Download file
    content = download_with_retry(url)
    if content is None:
        return False

    # Verify checksum
    # Use non-strict mode for documentation files (.txt, .html) as Lenovo
    # frequently updates these without updating checksums in metadata
    file_ext = name.lower().split('.')[-1] if '.' in name else ''
    is_documentation = file_ext in ['txt', 'html']
    if not verify_checksum(content, expected_crc, name, strict=not is_documentation):
        return False

    # Upload to release with retry
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Uploading {name} to release (attempt {attempt + 1}/{MAX_RETRIES})")

            # Create a file-like object for upload
            fileobj = io.BytesIO(content)

            # Use the upload function with autosplit for large files
            github.release_asset_upload_with_autosplit(
                release=release,
                name=name,
                fileobj=fileobj,
            )

            logger.info(f"Successfully uploaded {name}")
            return True
        except Exception as e:
            logger.warning(f"Upload failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to upload {name} after {MAX_RETRIES} attempts")
                return False

    return False


def update_metadata_asset(release: dict, xml_content: bytes) -> bool:
    """Update or create metadata asset in release."""
    try:
        # Try to delete existing metadata asset
        try:
            github.release_asset_delete(release, METADATA_ASSET_NAME)
            logger.info(f"Deleted old {METADATA_ASSET_NAME}")
        except Exception:
            # Asset might not exist, that's okay
            pass

        # Upload new metadata
        logger.info(f"Uploading {METADATA_ASSET_NAME}")
        fileobj = io.BytesIO(xml_content)
        github.release_asset_upload(
            release=release,
            name=METADATA_ASSET_NAME,
            fileobj=fileobj,
        )
        logger.info(f"Successfully uploaded {METADATA_ASSET_NAME}")
        return True
    except Exception as e:
        logger.error(f"Failed to update metadata asset: {e}")
        return False


def process_package(pkg_info: dict) -> bool:
    """Process a single package."""
    global has_errors

    metadata_url = pkg_info['location']
    expected_checksum = pkg_info['checksum']

    logger.info(f"=" * 80)
    logger.info(f"Processing package from {metadata_url}")

    # Download current metadata
    current_metadata = download_with_retry(metadata_url)
    if current_metadata is None:
        has_errors = True
        return False

    # Verify metadata checksum
    if not verify_checksum(current_metadata, expected_checksum, 'metadata'):
        has_errors = True
        return False

    # Parse metadata
    pkg_data = parse_package_metadata(current_metadata)
    if pkg_data is None:
        has_errors = True
        return False

    package_id = pkg_data['id']
    title = pkg_data['title']
    release_date = pkg_data['release_date']
    current_files = pkg_data['files']

    logger.info(f"Package ID: {package_id}")
    logger.info(f"Title: {title}")
    logger.info(f"Version: {pkg_data['version']}")
    logger.info(f"Release Date: {release_date}")
    logger.info(f"Files: {len(current_files)}")

    # Check if release exists
    release = github.release_get_by_tag(package_id)

    if release:
        logger.info(f"Release exists for {package_id}")

        # Get stored metadata
        stored_metadata = get_stored_metadata(release)

        if stored_metadata:
            # Compare checksums
            stored_checksum = compute_sha256(stored_metadata)
            current_checksum = compute_sha256(current_metadata)

            if stored_checksum == current_checksum:
                logger.info(f"Package {package_id} unchanged, skipping")
                return True

            logger.info(f"Package {package_id} changed, comparing files")

            # Parse stored metadata to compare files
            stored_pkg_data = parse_package_metadata(stored_metadata)
            if stored_pkg_data:
                stored_files = stored_pkg_data['files']
                files_to_download = compare_files(stored_files, current_files)
            else:
                # If we can't parse stored metadata, download all files
                files_to_download = current_files
        else:
            # No stored metadata, treat as new
            logger.info(f"No stored metadata for {package_id}, downloading all files")
            files_to_download = current_files
    else:
        logger.info(f"Creating new release for {package_id}")

        # Parse release date for git tag timestamp
        from datetime import datetime
        timestamp = None
        if release_date:
            try:
                timestamp = datetime.strptime(release_date, "%Y-%m-%d")
            except Exception as e:
                logger.warning(f"Failed to parse release date: {e}")

        # Create release
        try:
            release = github.release_create(
                tag_name=package_id,
                name=title,
                timestamp=timestamp,
            )
            logger.info(f"Created release for {package_id}")
        except Exception as e:
            logger.error(f"Failed to create release: {e}")
            has_errors = True
            return False

        files_to_download = current_files

    # Download and upload files
    if files_to_download:
        logger.info(f"Downloading and uploading {len(files_to_download)} files")

        for file_info in files_to_download:
            success = download_and_upload_file(release, file_info)
            if not success:
                logger.error(f"Failed to process file: {file_info['name']}")
                has_errors = True
                # Continue with other files
    else:
        logger.info("No files to download")

    # Update metadata asset
    success = update_metadata_asset(release, current_metadata)
    if not success:
        has_errors = True

    logger.info(f"Finished processing package {package_id}")
    return True


def main():
    """Main entry point."""
    global has_errors

    logger.info("Starting Lenovo driver crawler")

    try:
        # Load configuration
        config = load_config()
        machine_type = config['machine_type']
        os_type = config['os']

        logger.info(f"Configuration: Machine Type={machine_type}, OS={os_type}")

        # Build catalog URL
        catalog_url = f"https://download.lenovo.com/catalog/{machine_type}_{os_type}.xml"
        logger.info(f"Catalog URL: {catalog_url}")

        # Download catalog
        catalog_content = download_with_retry(catalog_url)
        if catalog_content is None:
            logger.error("Failed to download catalog")
            sys.exit(1)

        # Parse catalog
        packages = parse_catalog(catalog_content)
        if not packages:
            logger.error("No packages found in catalog")
            sys.exit(1)

        # Process each package
        for idx, pkg_info in enumerate(packages, 1):
            logger.info(f"Processing package {idx}/{len(packages)}")
            try:
                process_package(pkg_info)
            except Exception as e:
                logger.error(f"Unexpected error processing package: {e}", exc_info=True)
                has_errors = True
                # Continue with next package

        logger.info("=" * 80)
        logger.info("Crawler finished")

        if has_errors:
            logger.error("Crawler completed with errors")
            sys.exit(1)
        else:
            logger.info("Crawler completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
