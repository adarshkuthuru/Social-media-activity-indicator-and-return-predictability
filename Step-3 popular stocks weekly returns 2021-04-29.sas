libname RA 'C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social'; run;


/*%let wrds=wrds-cloud.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=adarshkp password= ;              
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname crsp '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname compa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname compq '/wrds/comp/sasdata/d_na'; run; *refers to comp quarterly;
libname cmpny '/wrds/comp/sasdata/d_na/company'; run; *refers to company file;
libname pn '/wrds/comp/sasdata/d_na/pension'; run; *compustat pension;
libname ibes '/wrds/ibes/sasdata'; run; *IBES;
libname frb '/wrds/frb/sasdata'; run; *FRB;
libname home '/home/bc/adarshkp/corp fin'; run;
libname temp '/scratch/bc/Adarsh'; run;
endrsubmit;*/


***************************************************************************************************************************************************************************
***************************************************************************************************************************************************************************

																	PART-2: ANALYSIS

***************************************************************************************************************************************************************************
**************************************************************************************************************************************************************************;



*************************************************************************************
							STEP-1: UNIQUE DAILY DATES IN THE DATASET
************************************************************************************;

proc sort data=cc nodupkey out=dates; by Date; run;
*126 obs;

data dates; set dates; if missing(date)=1 then delete; keep date; run;

data dates; set dates; id=1; run;


*************************************************************************************
									STEP-2: Form Date*Company Dataset
************************************************************************************;

**Import tsv files;
PROC IMPORT OUT= Identifiers
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\Ticker_Identifiers_2020-10-01_2021-03-31.tsv" 
            DBMS=TAB REPLACE;
RUN;
*2990 obs;

data identifiers; set identifiers; id=1; run;
proc sql;
	create table Date_Comp as
	select distinct a.date, b.ticker, b.ric, b.isin
	from dates as a left join identifiers as b
	on a.id=b.id
	order by b.ric,a.date;
quit;
*373750 obs;


*************************************************************************************
				STEP-3: Merge Russell returns with each social dataset
************************************************************************************;

*Step-1: Merge social indicator datasets with Dates and Russell returns files;
proc sql;
	create table cc1 as
	select distinct a.*, b.*
	from date_comp as a left join cc as b
	on a.date=b.date and a.ric=b.ric;
quit;
*373750 obs;

proc sql;
  create table cc1 as
  select distinct a.*, b.*
  from cc1 as a left join russell_final as b
  on a.ric=b.ric and a.Date=b.Date and missing(a.Date)=0
  order by a.ric, a.Date;
quit;
*373750 obs;

proc sql;
	create table co1 as
	select distinct a.*, b.*
	from date_comp as a left join co as b
	on a.date=b.date and a.ric=b.ric;
quit;
*373750 obs;

proc sql;
  create table co1 as
  select distinct a.*, b.*
  from co1 as a left join russell_final as b
  on a.ric=b.ric and a.Date=b.Date and missing(a.Date)=0
  order by a.ric, a.Date;
quit;
*373750 obs;

proc sql;
	create table oc1 as
	select distinct a.*, b.*
	from date_comp as a left join oc as b
	on a.date=b.date and a.ric=b.ric;
quit;
*373750 obs;

proc sql;
  create table oc1 as
  select distinct a.*, b.*
  from oc1 as a left join russell_final as b
  on a.ric=b.ric and a.Date=b.Date and missing(a.Date)=0
  order by a.ric, a.Date;
quit;
*373750 obs;

proc sql;
	drop table identifiers, cc, co, oc;
quit;


*************************************************************************************
					STEP-4: Estimate S&p500 returns weekly
************************************************************************************;

 data WORK.SP500    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\S&P 500 Historical Data.csv' delimiter = ',' MISSOVER DSD lrecl=13106 firstobs=2 ;
         informat Date mmddyy10. ;
         informat Price 10. ;
         informat Open 10. ;
         informat High 10. ;
         informat Low 10. ;
         informat Vol_ 3. ;
         informat Change__ 8. ;
         format Date mmddyy10. ;
         format Price 10. ;
         format Open 10. ;
         format High 10. ;
         format Low 10. ;
         format Vol_ 3. ;
         format Change__ 8. ;
      input
                  Date
                  Price  
                  Open  
                  High  
                  Low  
                  Vol_  
                  Change__  
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

proc sort data=SP500; by date; run;

*create past one week social indicator value;
data SP5002; 
	format date YYMMDD10. ric $7.  day 2. day_7 YYMMDD10.;
	set SP500;
	ret_cc=log(price/lag(price));
	ret_oc=log(price/open);
	ret_co=log(open/lag(price));
	day=weekday(date);
	week=week(date);
	Day_7=intnx("day",Date,-7);
run;
	
proc sql;
  create table SP5002a as
  select distinct a.*, sum(b.ret_cc) as ret7_cc, sum(b.ret_co) as ret7_co, sum(b.ret_oc) as ret7_oc
  from SP5002 as a left join SP5002 as b
  on a.Day_7<b.Date<=a.Date
  group by a.Date
  order by a.Date;
quit;

data SP5002a;
	set SP5002a;
	if day>4 then delete;
run;

proc sql;
	create table SP5002b as
	select distinct date, week, day, ret7_cc, ret7_co, ret7_oc
	from SP5002a
	group by week
	having day=max(day);
quit;

*************************************************************************************
					STEP-5: Form Portfolios - weekly
************************************************************************************;


*Step-2: Estimate the number of missing and non-missing companies by adte;
/*proc sql;
	create table test as
	select distinct date, start_date, count(distinct ric) as count
	from social1
	group by date, start_date;
quit;
*250 rows;

data test; 
	set test; 
	if missing(oc_date)=1 then data=0; 
	else data=1; 
run;*/


*Create a dataset with list of social indicators;
data variables;
	input variable $40.;
	datalines;
Social_Relative_Intensity 
Social_Intensity
Submission_Count 
Comment_Count 
Social_Sentiment 
Financial_Sentiment  
Non_Financial_Sentiment  
Social_Interaction  
Squeeze_Intensity__Text_ 
YOLO_Intensity__Text_ 
Rocket_Intensity__Text_ 
Moon_Intensity__Text_ 
Buy_Intensity__Text_
Sell_Intensity__Text_
Hold_Intensity__Text_
Short_Intensity__Text_
Crash_Intensity__Text_
Bag_Holder_Intensity__Text_
Dump_Intensity__Text_ 
Lost_Intensity__Text_
Drop_Intensity__Text_
_10_Bagger_Intensity__Text_  
Gamma_Squeeze_Intensity 
Debt_Intensity__Text_  
Options_Intensity__Text_  
Put_Options_Intensity__Text_  
Call_Options_Intensity__Text_  
Implied_Volatility_Intensity  
Volatility_Intensity__Text_  
Diamond_Intensity__Emoji_  
Rocket_Intensity__Emoji_  
Gain_Intensity__Flair_  
Loss_Intensity__Flair_  
DD_Intensity__Flair_  
YOLO_Intensity__Flair_  
Options_Intensity__Flair_  
Discussion_Intensity__Flair_  
News_Intensity__Flair_  
PNL__Flair_ 
;
run;

data variables; set variables; nrow=_N_; variable=trim(variable); run;

proc sql noprint;
        select count(*) into :num from variables;
quit;

*number of popular stocks dataset;
data pop;
	input nstock 3. nrow 2.;
	datalines;
10 1
50 2
100 3
;
run;

proc sql noprint;
        select count(*) into :num2 from pop;
quit;

/*data cc2;
	set cc1;
	if trim(ticker) in ('TSLA','GME','PLTR','AAPL','ASO','BB','VIAC','NIO','RKT','AMC','AMD','MVIS','AMZN','APHA','DIS','BABA','NKLA','MSFT','ZM','GM','BYND','GE','FB');
run;*/

*Assign number to num;
%let num = 24;
%put &num;


*1. Close-to-close;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=&num %to &num;  *Based on comment count;


%macro doit2;
%do j=1 %to &num2;

%put &nstock;

*Method-1: obtain daily portfolio returns for high, low and missing portfolios;
proc sql noprint;
        select variable into :var from variables where nrow=&i;
quit;

proc sql noprint;
        select nstock into :nstock from pop where nrow=&j;
quit;

*Create the dataset to form portfolios;
proc sql;
  create table social2 as
  select distinct date, ticker, ric, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, %trim(&var)
  from cc1;
quit;

data social2; set social2; nrow=_N_; if %trim(&var)='NaN' then %trim(&var)=.; run;
data social2; set social2; var1=input(%trim(&var),best12.); run;

*create past one week social indicator value;
data social2; 
	format date YYMMDD10. ric $7.  day 2. day_7 YYMMDD10.;
	set social2;
	day=weekday(date);
	week=week(date);
	Day_7=intnx("day",Date,-7);
run;
	
proc sql;
  create table social2a as
  select distinct a.*, sum(b.var1) as sum_var1
  from social2 as a left join social2 as b
  on a.ric=b.ric and a.Day_7<b.Date<=a.Date
  group by a.ric, a.Date
  order by a.ric, a.Date;
quit;

data social2a;
	set social2a;
	if day>4 then delete;
run;

proc sql;
	create table social2b as
	select distinct date, ticker, ric, week, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, var1, sum_var1, nrow
	from social2a
	group by ric, week
	having day=max(day);
quit;
proc sort data=social2b; by ric week date; run;

*create dataset with missing social indicator values;
data social3; set social2b; if missing(sum_var1)=1; Rank_Anomaly=2; run; 

*put rest into another dataset;
proc sql;
	create table social4 as
	select distinct *
	from social2b
	where nrow not in (select nrow from social3);
quit;

proc sort data=social4; by date sum_var1; run;
proc rank data=social4 groups=1 out=social4;
  by date;
  var sum_var1;
  ranks Rank_Anomaly;
run;

proc append data=social3 base=social4; run;

data social4; 
	format AnomalyVar $40. Rank_Anomaly;
	set social4; 
	AnomalyVar="%trim(&var)"; 
run;

proc sort data=social4 out=social4; by date descending sum_var1; run; 

data social4;
	set social4;
	by date;
	srno + 1;
	if first.date then srno = 1;
run;

*picks top 10 stocks;
data social4;
	set social4;
	if srno < &nstock+1;
run;


***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, max(srno) as nstocks, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;

data social_Port_&j;
	set social_Port;
	if Rank_Anomaly=2 then delete;
run;

%if &j>1 %then %do;
	proc sql;
		create table final_cc as 
		select distinct a.*, b.EWHoldRet as EWHoldRet_&j, b.VWHoldRet as VWHoldRet_&j
		from final_cc as a left join social_Port_&j as b
		on a.date=b.date;
	quit;
	%end;
  %else %do;
	data final_cc;
		set social_Port_&j;
	run;
	%end;
/*proc append data=TestStats base=Final_CC2; run;
proc append data=TestStats1 base=Final_CC3; run; */

proc sql;
	drop table social2, social2a, social2b, social3, social4, social5, social6, meanret, tstatret, mean, tstat, social_Port_&j, social_Port ;
quit;

%end;
%mend doit2;
%doit2

%end;
%mend doit1;
%doit1

data final_cc;
	set final_cc;
	drop Rank_Anomaly nstocks;
run;

**append 7 day s&p500 ret;
proc sql;
  create table Final_CC as
  select distinct a.*, b.ret7_cc
  from Final_CC as a left join sp5002b as b
  on a.Date=b.Date and missing(a.Date)=0
  order by a.Date;
quit;

proc sort data=final_cc; by nstocks date; run;
proc export data=Final_CC outfile="C:\Users\KUTHURU\Desktop\data.csv" 
dbms=csv replace;
run;














*2. Close-to-open;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=&num %to &num; *&num;  *Based on comment count;


%macro doit2;
%do j=1 %to &num2;

%put &nstock;

*Method-1: obtain daily portfolio returns for high, low and missing portfolios;
proc sql noprint;
        select variable into :var from variables where nrow=&i;
quit;

proc sql noprint;
        select nstock into :nstock from pop where nrow=&j;
quit;

*Create the dataset to form portfolios;
proc sql;
  create table social2 as
  select distinct date, ticker, ric, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, %trim(&var)
  from co1;
quit;

data social2; set social2; nrow=_N_; if %trim(&var)='NaN' then %trim(&var)=.; run;
data social2; set social2; var1=input(%trim(&var),best12.); run;

*create past one week social indicator value;
data social2; 
	format date YYMMDD10. ric $7.  day 2. day_7 YYMMDD10.;
	set social2;
	day=weekday(date);
	week=week(date);
	Day_7=intnx("day",Date,-7);
run;
	
proc sql;
  create table social2a as
  select distinct a.*, sum(b.var1) as sum_var1
  from social2 as a left join social2 as b
  on a.ric=b.ric and a.Day_7<b.Date<=a.Date
  group by a.ric, a.Date
  order by a.ric, a.Date;
quit;

data social2a;
	set social2a;
	if day>4 then delete;
run;

proc sql;
	create table social2b as
	select distinct date, ticker, ric, week, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, var1, sum_var1, nrow
	from social2a
	group by ric, week
	having day=max(day);
quit;
proc sort data=social2b; by ric week date; run;

*create dataset with missing social indicator values;
data social3; set social2b; if missing(sum_var1)=1; Rank_Anomaly=2; run; 

*put rest into another dataset;
proc sql;
	create table social4 as
	select distinct *
	from social2b
	where nrow not in (select nrow from social3);
quit;

proc sort data=social4; by date sum_var1; run;
proc rank data=social4 groups=1 out=social4;
  by date;
  var sum_var1;
  ranks Rank_Anomaly;
run;

proc append data=social3 base=social4; run;

data social4; 
	format AnomalyVar $40. Rank_Anomaly;
	set social4; 
	AnomalyVar="%trim(&var)"; 
run;

proc sort data=social4 out=social4; by date descending sum_var1; run; 

data social4;
	set social4;
	by date;
	srno + 1;
	if first.date then srno = 1;
run;

*picks top 10 stocks;
data social4;
	set social4;
	if srno < &nstock+1;
run;


***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, max(srno) as nstocks, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;

data social_Port_&j;
	set social_Port;
	if Rank_Anomaly=2 then delete;
run;

%if &j>1 %then %do;
	proc sql;
		create table final_co as 
		select distinct a.*, b.EWHoldRet as EWHoldRet_&j, b.VWHoldRet as VWHoldRet_&j
		from final_co as a left join social_Port_&j as b
		on a.date=b.date;
	quit;
	%end;
  %else %do;
	data final_co;
		set social_Port_&j;
	run;
	%end;
/*proc append data=TestStats base=Final_CC2; run;
proc append data=TestStats1 base=Final_CC3; run; */

proc sql;
	drop table social2, social2a, social2b, social3, social4, social5, social6, meanret, tstatret, mean, tstat, social_Port_&j, social_Port ;
quit;

%end;
%mend doit2;
%doit2

%end;
%mend doit1;
%doit1

data final_co;
	set final_co;
	drop Rank_Anomaly nstocks;
run;

**append 7 day s&p500 ret;
proc sql;
  create table Final_CO as
  select distinct a.*, b.ret7_co
  from Final_CO as a left join sp5002b as b
  on a.Date=b.Date and missing(a.Date)=0
  order by a.Date;
quit;

proc sort data=final_co; by nstocks date; run;
proc export data=Final_CO outfile="C:\Users\KUTHURU\Desktop\data1.csv" 
dbms=csv replace;
run;


















*3. Open-to-close;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=&num %to &num;  *Based on comment count;


%macro doit2;
%do j=1 %to &num2;

%put &nstock;

*Method-1: obtain daily portfolio returns for high, low and missing portfolios;
proc sql noprint;
        select variable into :var from variables where nrow=&i;
quit;

proc sql noprint;
        select nstock into :nstock from pop where nrow=&j;
quit;

*Create the dataset to form portfolios;
proc sql;
  create table social2 as
  select distinct date, ticker, ric, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, %trim(&var)
  from oc1;
quit;

data social2; set social2; nrow=_N_; if %trim(&var)='NaN' then %trim(&var)=.; run;
data social2; set social2; var1=input(%trim(&var),best12.); run;

*create past one week social indicator value;
data social2; 
	format date YYMMDD10. ric $7.  day 2. day_7 YYMMDD10.;
	set social2;
	day=weekday(date);
	week=week(date);
	Day_7=intnx("day",Date,-7);
run;
	
proc sql;
  create table social2a as
  select distinct a.*, sum(b.var1) as sum_var1
  from social2 as a left join social2 as b
  on a.ric=b.ric and a.Day_7<b.Date<=a.Date
  group by a.ric, a.Date
  order by a.ric, a.Date;
quit;

data social2a;
	set social2a;
	if day>4 then delete;
run;

proc sql;
	create table social2b as
	select distinct date, ticker, ric, week, day, ret1, ret7, MarketCapitalization, avg_MarketCapitalization, var1, sum_var1, nrow
	from social2a
	group by ric, week
	having day=max(day);
quit;
proc sort data=social2b; by ric week date; run;

*create dataset with missing social indicator values;
data social3; set social2b; if missing(sum_var1)=1; Rank_Anomaly=2; run; 

*put rest into another dataset;
proc sql;
	create table social4 as
	select distinct *
	from social2b
	where nrow not in (select nrow from social3);
quit;

proc sort data=social4; by date sum_var1; run;
proc rank data=social4 groups=1 out=social4;
  by date;
  var sum_var1;
  ranks Rank_Anomaly;
run;

proc append data=social3 base=social4; run;

data social4; 
	format AnomalyVar $40. Rank_Anomaly;
	set social4; 
	AnomalyVar="%trim(&var)"; 
run;

proc sort data=social4 out=social4; by date descending sum_var1; run; 

data social4;
	set social4;
	by date;
	srno + 1;
	if first.date then srno = 1;
run;

*picks top 10 stocks;
data social4;
	set social4;
	if srno < &nstock+1;
run;


***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, max(srno) as nstocks, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;

data social_Port_&j;
	set social_Port;
	if Rank_Anomaly=2 then delete;
run;

%if &j>1 %then %do;
	proc sql;
		create table final_oc as 
		select distinct a.*, b.EWHoldRet as EWHoldRet_&j, b.VWHoldRet as VWHoldRet_&j
		from final_oc as a left join social_Port_&j as b
		on a.date=b.date;
	quit;
	%end;
  %else %do;
	data final_oc;
		set social_Port_&j;
	run;
	%end;
/*proc append data=TestStats base=Final_CC2; run;
proc append data=TestStats1 base=Final_CC3; run; */

proc sql;
	drop table social2, social2a, social2b, social3, social4, social5, social6, meanret, tstatret, mean, tstat, social_Port_&j, social_Port ;
quit;

%end;
%mend doit2;
%doit2

%end;
%mend doit1;
%doit1

data final_oc;
	set final_oc;
	drop Rank_Anomaly nstocks;
run;
**append 7 day s&p500 ret;
proc sql;
  create table Final_OC as
  select distinct a.*, b.ret7_oc
  from Final_OC as a left join sp5002b as b
  on a.Date=b.Date and missing(a.Date)=0
  order by a.Date;
quit;

proc sort data=final_oc; by nstocks date; run;
proc export data=Final_OC outfile="C:\Users\KUTHURU\Desktop\data2.csv" 
dbms=csv replace;
run;





proc sql;
	drop table Final_CC, Final_CC2, Final_CC3, Final_CO, Final_CO2, Final_CO3, Final_OC, Final_OC2, Final_OC3;
quit;



