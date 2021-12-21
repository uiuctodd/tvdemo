# NETFLIX take-home tech screening for Todd Markle

## Discussion

Here is the problem as stated:

> You ask for some example questions.  She says:
> 
> - Are we licensing the right shows?  What should we be trying to add or 
>   remove from the catalog?
> - Is there anything we can do to improve customer engagement?
> - How engaged are our customers with our service?

Some of these questions seem to be about basic viewership numbers broken out on various dimensions:
* What genre of shows get the most viewing hours in Korea?
* Do Americans like to watch crime shows in the morning or the evening?
* Does the popularity of a show drop after it's stopped production?

Other questions might require building profiles of users:
* What shows are popular with multilingual viewers?
* Is this show popular with a small but loyal viewership?
* Do people like to binge-watch this show?

Bad ideas:
* Canceling a low-popularity show worldwide, but discovering it was huge in Japan.
* Canceling a low-popularity show only to discover that many viewers were only tuning in to
  that one show regularly. Overall viewership can go up while audience reach goes down.

The viewership questions are answered by data coming from simple roll-ups of the play events. Questions about user reach 
and viewing profiles are much more difficult. Getting these answers requires transforming the data from time-series 
to the user dimension. Though it is still "time series" data, hours cannot simply be added up.

## Open issue: episodes verses shows

A single episode id in the scheduling feed seems to be for an instance of a show being broadcast. That is, it's not "Friends season 4, ep 1", but rather the showing of that episode at (for example) 7pm tonight. I've gone back and forth on this interpretation. However, the broadcast time is in the episode record. There doesn't appear to be any synthetic 
entity for "a single showing of an episode".

For both viewership and user profiles, it's interesting to ask whether the data should be rolled at the show or 
episode level. I've decided to roll viewership at the episode level in order to be able to break stats down by 
time-of-day. However, I will roll user profiles to the show level.

## Open Issue: Maintenance of scheduling data

Looking at the scheduling feed, I can't determine when records go live and when they fall off. 
It seems to be stable for the current week or so, with some things scheduled in the far future and 
previous days rolling off. I am assuming that the data is stable around the time when we will be 
rolling it. So I will capture dimensional information for a day on that day. However, I have not 
implemented anything that would capture slow-moving dimensional data.

## Basic approach to the system

### use Spark to summarize data

A production system would presumable use something like AWS EMR. I have written my code to anticipate 
that. However, I have not tested it there. I've only tested Spark on my dev environment to show
basic awareness.

### use S3 to store data

Ten years ago, Spark would have immediately implied a Hadoop data store. However, many people are
choosing S3 recently. Buckets work well with parallel readers.

I have also not tested the system with S3. However, I've mostly chosen tools that can work interchangeably
with local file storage in the dev environment (Pandas, Spark Datasets, etc). The file system is configured
in the main INI file. It would be interesting to swap that out and see what fails....

### files, not database tables

The approach will be to store all data as files, and to design a datamart that can be built and loaded
for a particular time period. Sometimes I've referred to this as a "kiosk" approach. An analyst who wants
first quarter data can bring up a datamart and load just those files.

### use mysql for the datamart

This will be an interesting decision. My first reaction was, "throw all the data into a columnar DB like
Redshift". This would eliminate some of the need for normalization of data, as columnar databases 
deal well with repeating records.

Looking at the scale of the summary data, however, I decided that the summary data for a reasonable interval--
say one quarter-- fits into traditional RDBs. Also, the interview team might want to know if I can normalize data.

I chose mysql rather than postgres because the last time I used pg, it had no INSERT... ON DUPLICATE KEY
functionality. I was intending to use this for various ETL operations. Oddly, I never did. Mostly because
I ran out of time and did not have time to implement that bit.

## Implementation

### Kafka

I have not implemented a Kafka listener. Instead, I am assuming we will use Kafka connect with the S3 Sink.

The sink as a time-based partitioned. While time is not inside the event, there will be a "received time" in the 
envelope that can be used to partition files. Therefore I am assuming that the start-point for event data
is S3 files parted by date and hour. (The latter is just to control file size -- going from 100 million events 
down to a few million events per file).

While it would be possible to do the traffic stats directly off the pipeline, it would be more difficult to do
user profiles directly from streaming. So it feels like writing the data is the correct choice.

Presumably raw data files would be discarded some time after roll-ups were complete.

### Spark pipelines

I have two spark pipes. One generates the viewership stats. This one is working. The resulting data is ready
to be loaded into the datamart.

The second one is for user profiles. I have run out of time on this. It only does the first step, which
is to summarize an hour of viewership by user and show. This second pipeline would be the start of 
several aggregations based around show reach and audience profile.

### Datamart

A schema is in the "conf" directory for discussion. 

### capture of Dimensional (episode) data:

Two scripts (and supporting libraries) pull down data from the schedule service, normalize it a bit, and
write it to the file system by day. Two other scripts load this data and merge it into the dimension tables.

### Loading data

I've written some "interesting" etl for the dimensional information. it's a highly efficient way of loading.
Data is direct-path loaded into temporary memory tables and then flashed to the permanent location. I've also 
demonstrated one way that this can be used to do normalization of minor dimensions right in the  layer.

There are disadvantages to this approach. Firstly, it requires lots of code to be kept in sync. Secondly,
it will not work "out of the box" if the load files are in S3. This is the only glaring omission in the "write
for either the dev env or AWS" that I'm aware of.

An alternate way to load would be to use Pandas. I'm not sure about the methods available for dealing with
re-loading and merging from Pandas. It's very easy to control this homebrew.




