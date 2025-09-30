#!/usr/bin/env python3
import argparse, boto3, json, os, datetime as dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()

    sess = boto3.Session(profile_name=args.profile)
    sts = sess.client("sts")
    ident = sts.get_caller_identity()

    out = {
        "profile": args.profile,
        "account": ident.get("Account"),
        "arn": ident.get("Arn"),
        "userId": ident.get("UserId"),
        "at": dt.datetime.utcnow().isoformat() + "Z",
    }
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
