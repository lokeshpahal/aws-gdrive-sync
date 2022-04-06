from googleapiclient.http import MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from oauth2client import tools

import boto3
import datetime
import io
import mimetypes
import os
import sys
import time

from apiclient import errors
from apiclient import discovery

from logbook import Logger, FileHandler, StreamHandler

log = Logger('s3-to-google-drive')

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser])
    # add in our specific command line requirements
    flags.add_argument('--folder_id', '-f', type=str, required=False,
                       help="Google Drive Folder ID (it's the end of the folder URI!) (required)")
    flags.add_argument('--bucket', '-b', type=str, required=True,
                       help="Name of S3 bucket to use (required)")
    flags.add_argument('--aws-id', '-id', type=str, required=True,
                       help="AWS Access key ID (required)")
    flags.add_argument('--aws-secret-key', '-key', type=str, required=True,
                       help="AWS Aecret Access key (required)")
    flags.add_argument('--key-prefix', '-k', type=str, required=True,
                       help="Key prefix to use as the path to a folder in S3 (required)")
    flags.add_argument('--page-size', '-p', type=int, default=100,
                       help="Number of files in each page (defaults to 100)")
    flags.add_argument('--start-page', '-s', type=int, default=1,
                       help="start from page N of the file listing (defaults to 1)")
    flags.add_argument('--end-page', '-e', type=int, default=None,
                       help="stop paging at page N of the file listing (defaults to not stop before the end)")
    flags.add_argument('--match-file', type=str, default=None,
                       help="Only process files if the filename is in this file (defaults to process all files)")
    flags.add_argument('--log-dir', '-l', type=str, help='Where to put log files', default='./')
    flags.add_argument('--log-level', type=str, help='Choose a log level', default='INFO')
    args = flags.parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
# SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'

SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Spryng.io Google Drive to S3'


def get_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=8081)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds

def ensure_trailing_slash(val):
    if val[-1] != '/':
        return "{}/".format(val)
    return val

def we_should_process_this_file(filename, match_files):
    if not match_files:  # We have not supplied any file names to match against, so process everything.
        return True

    try:
        filename = filename.split('/')[-1]
    except IndexError:
        return False

    if filename in match_files:
        return True
    return False

def insert_file(service, title, fh):
    mimetype = mimetypes.guess_type(title)[0]
    media_body = MediaIoBaseUpload(
        fh,
        mimetype=mimetype,
        chunksize=1024*1024,
        resumable=True
    )
    body = {'name': title}

    try:
        file = service.files().create(body=body, media_body=media_body).execute()
        return file
    except errors.HttpError as error:
        log.error('An error occurred: %s' % error)
        return None

def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """

    log_filename = os.path.join(
        args.log_dir,
        's3-to-google-drive-{}.log'.format(os.path.basename(time.strftime('%Y%m%d-%H%M%S')))
    )

    # register some logging handlers
    log_handler = FileHandler(
        log_filename,
        mode='w',
        level=args.log_level,
        bubble=True
    )
    stdout_handler = StreamHandler(sys.stdout, level=args.log_level, bubble=True)

    with stdout_handler.applicationbound():
        with log_handler.applicationbound():
            log.info("Arguments: {}".format(args))
            start = time.time()
            log.info("starting at {}".format(time.strftime('%l:%M%p %Z on %b %d, %Y')))

            credentials = get_credentials()
            # http = credentials.authorize(httplib2.Http())
            drive_service = discovery.build('drive', 'v3', credentials=credentials)

            # load up a match file if we have one.
            if args.match_file:
                with open(args.match_file, 'r') as f:
                    match_filenames = f.read().splitlines()
            else:
                match_filenames = None

            s3 = boto3.client(
                's3',
                aws_access_key_id=args.aws_id,
                aws_secret_access_key=args.aws_secret_key
            )

            key_prefix = ensure_trailing_slash(args.key_prefix)
            paginator = s3.get_paginator('list_objects')
            operation_parameters = {
                'Bucket': args.bucket,
                'Prefix': key_prefix,
                'PaginationConfig': {'MaxItems': 10, 'PageSize': 1}
            }

            page_counter = 0
            file_counter = 0

            result = paginator.paginate(**operation_parameters)
            for page in result:
                page_counter += 1
                page_file_counter = 0  # reset the paging file counter

                for this_file in page.get('Contents', []):
                    if we_should_process_this_file(this_file['Key'], match_filenames):
                        filename = this_file['Key'].split('/')[-1]
                        log.info(u"#== Processing {} file number {} on page {}. {} files processed.".format(
                            this_file['Key'],
                            page_file_counter,
                            page_counter,
                            file_counter
                        ))
                        fh = io.BytesIO()  # Using an in memory stream location
                        s3.download_fileobj(args.bucket, this_file['Key'], fh)

                        log.info(u"Uploading to drive")
                        insert_file(
                            drive_service, filename, fh
                        )
                        log.info(u"Uploaded to drive")
                        fh.close()

                if args.end_page and page_counter == args.end_page:
                    log.info(u"Finished paging at page {}".format(page_counter))
                    break

            log.info("Running time: {}".format(str(datetime.timedelta(seconds=(round(time.time() - start, 3))))))
            log.info("Log written to {}:".format(log_filename))


if __name__ == '__main__':
    main()
