# Stripe, Discord, DBT

create a repo, deploy ssh key and use GITHub instead deploying container to dockerhub

[mounting DAGs from a private GitHub repo using Git-Sync sidecar](https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html#mounting-dags-from-a-private-github-repo-using-git-sync-sidecar)

tip: You have to convert the private ssh key to a base64 string. You can convert the private ssh key file like so:

```docker
base64 < ~/.ssh/id_rsa -w 0 > temp.txt
```

created file `override-values.yaml`

```yaml
dags:
  gitSync:
    enabled: true
    repo: git@github.com:lucasudar/kubernetes_surfalytics.git
    branch: master
    subPath: ""
    sshKeySecret: airflow-ssh-secret
extraSecrets:
  airflow-ssh-secret:
    data: |
      gitSshKey: '<base64-converted-ssh-private-key>'
```

Finally, from the context of your Airflow Helm chart directory, you can install Airflow:

```bash
helm upgrade --install airflow apache-airflow/airflow -f override-values.yaml
```