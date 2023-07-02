# Social Media Ingestion

This repository contains scripts to ingest social media data from a variety of platforms.


## Discord
To add additional servers:

1. login to discord:
	- user: mattburke
	- pass: DxhG8Pl0Z

2. Join a new server

3. right click server icon 

4. Copy ID

5. Add ID to guild dictionary in Discord.py



## Get crontab from fresh QO

To install crontab on the Quant Office Jupyterhub instance:

1. ```ssh -i wavebridge-trading.pem centos@172.16.111.119```

2. Find the process ID for jupyter hub:

```docker ps```

3. Enter process as root user:

```docker exec -u 0 -it [process id] bash```

4. Download crontab

```apt-get install crontab``` 

5. Start cron process

```service cron start```
```service cron status```


## Add script to crontab schedule

```chmod a+x [script to schedule]```

Make sure that script is being executed from same environment as usual
```#in python script add os.chdir('/tmp/jupyterhub/admin/[folder of script]')```

```crontab -e```

crontab file: see https://crontab.guru/ for how to write the schedule

```1 0 * * * /opt/conda/bin/python3 /tmp/jupyterhub/admin/Data/SocialMedia/Discord.py >> /tmp/jupyterhub/admin/Data/SocialMedia/discord.log 2>&1```

```0 * * * * /opt/conda/bin/python3 /tmp/jupyterhub/admin/Data/SocialMedia/livetwitter.py >> /tmp/jupyterhub/admin/Data/SocialMedia/livetwitter.log 2>&1```



## TO DO:
1. Major speedup will come from the sentiment predictor e.g. use quantization or a pruned model

