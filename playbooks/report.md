# TED Data Report Playbook

1. Log in to NIoT VM via RDP/ssh
2. Switch to opensafely user: `sudo su - opensafely`
3. Switch to directory: `cd os-schools-data`, and `git pull`.
4. Extract csv files: `just extract-all`
5. Generate report: `just report-real`, will be in `/srv/medium_privacy/workspaces/reports/real.html`
6. Generate raw report: `just report-raw`, will be in `/srv/medium_privacy/workspaces/reports/data.html 

In one command: `just extract-all report-real report-raw`

# Output checking

Shaun can get a copy of the file running the following in a terminal, and check
it and manually release it to sharepoint as needed.

`scp shaun@10.10.80.68:/srv/medium_privacy/workspaces/reports/*.html .`
