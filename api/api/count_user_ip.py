from pathlib import Path

log_dir = "/var/log/nginx"
prefix = "nedrex.log"

ip_addrs = set()

for f in Path(log_dir).iterdir():
    if not f.name.startswith(prefix):
        continue
    split = f.name.replace(prefix, "").split(".")
    # Name of the file is nedrex.log
    if split == [""]:
        pass
    # Name of the file is nedrex.log.
    elif int(split[1]) in range(1, 52) and (len(split) == 2 or split[2] == ".gz"):
        pass
    else:
        continue

    # Process files here.
    with f.open("r") as log:
        for line in log:
            ip_addrs.add(line.strip().split()[0])

print(f"There have been {len(ip_addrs)} unique IP addresses accessing the API in the last 12 months.")
