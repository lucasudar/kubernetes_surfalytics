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
    subPath: "./discord+stripe+dbt/dags"
    sshKeySecret: airflow-ssh-secret
    knownHosts: |
      github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
extraSecrets:
  airflow-ssh-secret:
    data: |
      gitSshKey: LS0tLS1CRUdJTiBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0KYjNCbGJuTnphQzFyWlhrdGRqRUFBQUFBQkc1dmJtVUFBQUFFYm05dVpRQUFBQUFBQUFBQkFBQUFNd0FBQUF0emMyZ3RaVwpReU5UVXhPUUFBQUNDZmg1YmdpajN6aXdhTlo0N2lHbVJCNnkrWlVuUlY4S1JQL3ptZzRjQktUUUFBQUtpdGpra2xyWTVKCkpRQUFBQXR6YzJndFpXUXlOVFV4T1FBQUFDQ2ZoNWJnaWozeml3YU5aNDdpR21SQjZ5K1pVblJWOEtSUC96bWc0Y0JLVFEKQUFBRUFvRzRCK0toaWZqMUxrSFBOdklJck9RcC8rdkdxdTZncndvSStoNjh2MFo1K0hsdUNLUGZPTEJvMW5qdUlhWkVIcgpMNWxTZEZYd3BFLy9PYURod0VwTkFBQUFKRjlzZFdOaGMxOUFUbWxyYVhSaGN5MU5ZV05DYjI5ckxVRnBjaTFOTVM1c2IyCk5oYkFFPQotLS0tLUVORCBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0K
```

Finally, from the context of your Airflow Helm chart directory, you can install Airflow:

```bash
helm upgrade --install airflow apache-airflow/airflow -f override-values.yaml
```

to recreate helm chart:

```bash
helm upgrade --install airflow apache-airflow/airflow -f override-values.yaml --namespace airflow
```

```bash
kubectl port-forward svc/airflow-webserver 8081:8080 --namespace airflow
```

you are usingÂ `dags.gitSync.sshKeySecret`, you should also [set](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#knownhosts) `dags.gitSync.knownHosts`