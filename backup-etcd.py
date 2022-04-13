#!python3
import os
import urllib.request
import subprocess
from datetime import datetime
from pathlib import Path
import argparse


def download_url(url, filename, force=False):
    """Download a file from a url and place it in root.
    Args:
        url (str): URL to download file from
        filename (str): Name to save the file under. If None, use the basename of the URL
        force (bool): Redownload the file
    """

    path = Path(filename)
    if path.is_file():
        return

    print("Downloading " + url + " to " + str(path))
    urllib.request.urlretrieve(url, path)
    path.chmod(0o755)


def auth_mc(name, url, key, secret):
    # mc alias set ALIAS URL ACCESSKEY SECRETKEY
    res = subprocess.run(["./mc", "alias", "set", name, url, key, secret])
    res.check_returncode()
    return res


def upload(name, bucket, src, dest):
    res = subprocess.run(["./mc", "cp", src, f"{name}/{bucket}/{dest}"])
    res.check_returncode()
    return res


def prep_upload(alias, key, secret):
    mc_url = "https://dl.minio.io/client/mc/release/linux-amd64/mc"
    download_url(mc_url, "./mc")
    auth_mc(alias, "https://s3.eu-west-1.amazonaws.com", key, secret)


def etcd_backup_dir(etcd_cluster_name, basedir="etcd-backups"):
    date_time = datetime.utcnow()
    s3path = f"{etcd_cluster_name}/{date_time.year}/{date_time.month}/{etcd_cluster_name}-{date_time.date()}_{date_time.time()}-snapshot.db"
    h_index = date_time.hour % 3
    d_index = date_time.day % 2
    p = Path(f"{basedir}/{etcd_cluster_name}")
    p.mkdir(parents=True, exist_ok=True)
    localpath = p.joinpath(f"{etcd_cluster_name}-snapshot-{d_index}-{h_index}.db")
    return [localpath, s3path]


def etcd_certs(server=None, key=None, cert=None, ca=None):
    os.environ.pop("ETCDCTL_KEY", None)
    os.environ.pop("ETCDCTL_CERT", None)
    os.environ.pop("ETCDCTL_CACERT", None)

    if server is None:
        server = os.uname().nodename
    if key is None:

        key = f"/etc/etcd/ssl/{server}/{server}-client-key.pem"
    if cert is None:
        cert = f"/etc/etcd/ssl/{server}/{server}-client.pem"
    if ca is None:
        ca = f"/etc/etcd/ssl/{server}/client-ca.pem"

    return {
        "key": key,
        "cert": cert,
        "ca": ca,
    }


def etcd_backup(dest, endpoints="https://127.0.0.1:2379", certs=None):
    if not certs:
        certs = etcd_certs()
    res = subprocess.run(
        [
            "/opt/bin/etcdctl",
            f"--endpoints={endpoints}",
            f"--cert={certs['cert']}",
            f"--cacert={certs['ca']}",
            f"--key={certs['key']}",
            "snapshot",
            "save",
            dest,
        ]
    )
    res.check_returncode()


def main():
    parser = argparse.ArgumentParser(description="Backup etcd and upload to s3")
    parser.add_argument(
        "--endpoints",
        default="https://127.0.0.1:2379",
        help="etcd endpoints to backup",
        required=False,
    )
    parser.add_argument(
        "--prefix", help="s3 prefix", default="etcd-backups", required=False
    )
    parser.add_argument(
        "--bucket", help="s3 bucket", default="pg-conny-backups", required=False
    )
    parser.add_argument("--cluster-name", help="etcd-cluster name", required=True)
    parser.add_argument(
        "--backup-dir", help="backup-dir", default="./etcd-backups", required=False
    )
    parser.add_argument("--key", help="etcd cert client key", required=False)
    parser.add_argument("--cert", help="path to etcd cert", required=False)
    parser.add_argument("--ca", help="path to etcd CA cert", required=False)
    parser.add_argument("--s3-access", help="AWS-S3 access key", required=True)
    parser.add_argument("--s3-secret", help="AWS-S3 Secret", required=True)

    args = parser.parse_args()
    alias = "s3"

    # Download mc and auth S3
    prep_upload(alias, args.s3_access, args.s3_secret)
    # Create local directory to store the backup
    localpath, s3path = etcd_backup_dir(args.cluster_name, args.backup_dir)
    # Execute etcd backup with etcdctl
    etcd_backup(localpath, args.endpoints, etcd_certs())
    # Upload the backup to s3
    upload(alias, "pg-conny-backups", localpath, f"{args.prefix}/{s3path}")


main()
