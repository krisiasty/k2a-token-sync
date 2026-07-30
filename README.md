# r2a-cert-sync
A lightweight, long-running Go daemon that monitors downstream RKE2 cluster certificates via Rancher, automates their rotation, and atomically syncs the updated credentials into direct-access ArgoCD cluster secrets.
