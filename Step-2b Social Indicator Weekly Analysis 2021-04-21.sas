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
	drop table identifiers, cc, co, oc, russell_final;
quit;


*************************************************************************************
					STEP-4: Form Portfolios - daily
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



*1. Close-to-close;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to &num;




*Method-1: obtain daily portfolio returns for high, low and missing portfolios;

proc sql noprint;
        select variable into :var from variables where nrow=&i;
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
proc rank data=social4 groups=2 out=social4;
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

***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;





*Method-2: obtain high minus low and high minus missing portfolio returns;

*create high minus low factor;
proc sql;
  create table social5 as
  select distinct a.Date, a.AnomalyVar, 4 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=0)) as b
  where a.Date=b.Date;        
quit;

*create high minus missing factor;
proc sql;
  create table social6 as
  select distinct a.Date, a.AnomalyVar, 5 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=2)) as b
  where a.Date=b.Date;        
quit;

*Consolidate above portret in one dataset;
data social_port2;
  set social5 social6;
run;

***Mean, STD, T-stat;
proc sort data=social_port2; by Rank_Anomaly date; run;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=MeanRet mean=VWHoldRet EWHoldRet;
quit;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=TstatRet t=VWHoldRet EWHoldRet;
quit;

data MeanRet;
  set MeanRet;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data TstatRet;
  set TstatRet;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;

***Collect all results;
data TestStats;
  length Stat $ 10;
  set MeanRet TstatRet;
run;

proc sort data=TestStats; by Rank_Anomaly stat; run;





*Method-3: Fama-Macbeth regressions;


*include social indicator scaled by standard deviation of the cross-section;
proc sql;
	create table social4 as
	select distinct *, std(sum_var1) as std, sum_var1/std(sum_var1) as sum_var2
	from social4 
	group by date;
quit;

data social4;
	set social4;
	if missing(sum_var2)=1 or missing(ret7)=1 then delete;
run;

proc sort data=social4; by AnomalyVar date Rank_Anomaly; run;
proc reg data=social4 noprint tableout outest=Alpha_FMB;
  by AnomalyVar date;
  model Ret7 = sum_var2;
  model Ret7 = sum_var2;
quit;

data Alpha_FMB;
  set Alpha_FMB;
  where _Type_ in ('PARMS');
  keep AnomalyVar date _Type_ Intercept sum_var2;
  if _Type_='PARMS' then Intercept=Intercept;
  rename _Type_ =Stat;
run;

proc sort data=Alpha_FMB; by Rank_Anomaly date; run;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Mean mean=sum_var2;
quit;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Tstat t=sum_var2;
quit;

data Mean;
  set Mean;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data Tstat;
  set Tstat;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;
***Collect all results;
data TestStats1;
  length Stat $ 10;
  set Mean Tstat;
run;

proc sort data=TestStats1; by stat; run;


proc append data=social_Port base=Final_CC; run;
proc append data=TestStats base=Final_CC2; run;
proc append data=TestStats1 base=Final_CC3; run;

proc sql;
	drop table social2, social3, social4, social5, social6, meanret, tstatret, social_port2, mean, tstat;
quit; 

%end;
%mend doit1;
%doit1




*2. Close-to-open;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to &num;




*Method-1: obtain daily portfolio returns for high, low and missing portfolios;

proc sql noprint;
        select variable into :var from variables where nrow=&i;
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
proc rank data=social4 groups=2 out=social4;
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

***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;





*Method-2: obtain high minus low and high minus missing portfolio returns;

*create high minus low factor;
proc sql;
  create table social5 as
  select distinct a.Date, a.AnomalyVar, 4 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=0)) as b
  where a.Date=b.Date;        
quit;

*create high minus missing factor;
proc sql;
  create table social6 as
  select distinct a.Date, a.AnomalyVar, 5 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=2)) as b
  where a.Date=b.Date;        
quit;

*Consolidate above portret in one dataset;
data social_port2;
  set social5 social6;
run;

***Mean, STD, T-stat;
proc sort data=social_port2; by Rank_Anomaly date; run;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=MeanRet mean=VWHoldRet EWHoldRet;
quit;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=TstatRet t=VWHoldRet EWHoldRet;
quit;

data MeanRet;
  set MeanRet;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data TstatRet;
  set TstatRet;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;

***Collect all results;
data TestStats;
  length Stat $ 10;
  set MeanRet TstatRet;
run;

proc sort data=TestStats; by Rank_Anomaly stat; run;





*Method-3: Fama-Macbeth regressions;


*include social indicator scaled by standard deviation of the cross-section;
proc sql;
	create table social4 as
	select distinct *, std(sum_var1) as std, sum_var1/std(sum_var1) as sum_var2
	from social4 
	group by date;
quit;

data social4;
	set social4;
	if missing(sum_var2)=1 or missing(ret7)=1 then delete;
run;

proc sort data=social4; by AnomalyVar date Rank_Anomaly; run;
proc reg data=social4 noprint tableout outest=Alpha_FMB;
  by AnomalyVar date;
  model Ret7 = sum_var2;
  model Ret7 = sum_var2;
quit;

data Alpha_FMB;
  set Alpha_FMB;
  where _Type_ in ('PARMS');
  keep AnomalyVar date _Type_ Intercept sum_var2;
  if _Type_='PARMS' then Intercept=Intercept;
  rename _Type_ =Stat;
run;

proc sort data=Alpha_FMB; by Rank_Anomaly date; run;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Mean mean=sum_var2;
quit;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Tstat t=sum_var2;
quit;

data Mean;
  set Mean;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data Tstat;
  set Tstat;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;
***Collect all results;
data TestStats1;
  length Stat $ 10;
  set Mean Tstat;
run;


proc sort data=TestStats1; by stat; run;


proc append data=social_Port base=Final_CO; run;
proc append data=TestStats base=Final_CO2; run;
proc append data=TestStats1 base=Final_CO3; run;

proc sql;
	drop table social2, social3, social4, social5, social6, meanret, tstatret, social_port2, mean, tstat;
quit; 

%end;
%mend doit1;
%doit1


*3. Open-to-close;
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to &num;




*Method-1: obtain daily portfolio returns for high, low and missing portfolios;

proc sql noprint;
        select variable into :var from variables where nrow=&i;
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
proc rank data=social4 groups=2 out=social4;
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

***Obtain Portfolio Returns;
proc sql;
  create table social_Port as
  select distinct Date, AnomalyVar, Rank_Anomaly, mean(Ret7) as EWHoldRet, sum(MarketCapitalization*Ret7)/sum(MarketCapitalization) as VWHoldRet
  from social4
  group by Date, AnomalyVar, Rank_Anomaly
  order by Date, AnomalyVar, Rank_Anomaly;
quit;





*Method-2: obtain high minus low and high minus missing portfolio returns;

*create high minus low factor;
proc sql;
  create table social5 as
  select distinct a.Date, a.AnomalyVar, 4 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=0)) as b
  where a.Date=b.Date;        
quit;

*create high minus missing factor;
proc sql;
  create table social6 as
  select distinct a.Date, a.AnomalyVar, 5 as Rank_Anomaly, a.EWHoldRet-b.EWHoldRet as EWHoldRet, a.VWHoldRet-b.VWHoldRet as VWHoldRet
  from social_Port(where=(Rank_Anomaly=1)) as a, social_Port(where=(Rank_Anomaly=2)) as b
  where a.Date=b.Date;        
quit;

*Consolidate above portret in one dataset;
data social_port2;
  set social5 social6;
run;

***Mean, STD, T-stat;
proc sort data=social_port2; by Rank_Anomaly date; run;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=MeanRet mean=VWHoldRet EWHoldRet;
quit;
proc means data=social_port2 noprint;
  by AnomalyVar Rank_Anomaly;
  var VWHoldRet EWHoldRet;
  output out=TstatRet t=VWHoldRet EWHoldRet;
quit;

data MeanRet;
  set MeanRet;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data TstatRet;
  set TstatRet;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;

***Collect all results;
data TestStats;
  length Stat $ 10;
  set MeanRet TstatRet;
run;

proc sort data=TestStats; by Rank_Anomaly stat; run;





*Method-3: Fama-Macbeth regressions;


*include social indicator scaled by standard deviation of the cross-section;
proc sql;
	create table social4 as
	select distinct *, std(sum_var1) as std, sum_var1/std(sum_var1) as sum_var2
	from social4 
	group by date;
quit;

data social4;
	set social4;
	if missing(sum_var2)=1 or missing(ret7)=1 then delete;
run;

proc sort data=social4; by AnomalyVar date Rank_Anomaly; run;
proc reg data=social4 noprint tableout outest=Alpha_FMB;
  by AnomalyVar date;
  model Ret7 = sum_var2;
  model Ret7 = sum_var2;
quit;

data Alpha_FMB;
  set Alpha_FMB;
  where _Type_ in ('PARMS');
  keep AnomalyVar date _Type_ Intercept sum_var2;
  if _Type_='PARMS' then Intercept=Intercept;
  rename _Type_ =Stat;
run;

proc sort data=Alpha_FMB; by Rank_Anomaly date; run;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Mean mean=sum_var2;
quit;
proc means data=Alpha_FMB noprint;
  by AnomalyVar;
  var sum_var2;
  output out=Tstat t=sum_var2;
quit;

data Mean;
  set Mean;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='Mean';
run;
data Tstat;
  set Tstat;
  rename sum_var2=coeff;
  drop _Type_ _Freq_;
  Stat='T-Stat';
run;
***Collect all results;
data TestStats1;
  length Stat $ 10;
  set Mean Tstat;
run;

proc sort data=TestStats1; by stat; run;


proc append data=social_Port base=Final_OC; run;
proc append data=TestStats base=Final_OC2; run;
proc append data=TestStats1 base=Final_OC3; run;

proc sql;
	drop table social2, social3, social4, social5, social6, meanret, tstatret, social_port2, mean, tstat;
quit; 

%end;
%mend doit1;
%doit1



*************************************************************************************
					STEP-5: Extract Fama-French Daily factors from WRDS
************************************************************************************;

/*rsubmit;

*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan2020'd;
%let crspenddate = '15Apr2021'd; 

data ff;
	set ff.FACTORS_DAILY;
	if date > &crspbegdate and date< &crspenddate;
run;
proc download data=ff out=ff; run;
*291 obs;
endrsubmit;*/

**Import csv files;
PROC IMPORT OUT= ff
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Ff 5factors.csv" 
            DBMS=CSV REPLACE;
RUN;

data ff;
	set ff;
	date1=input(put(date,best8.),yymmdd8.);
	format date1 date9.;
	if year(date1)>2019;
run;



*************************************************************************************
				STEP-6: Merge Fama-French Daily factors with portfolio returns
************************************************************************************;

proc sql;
	create table Final_CC as
	select distinct a.*, b.*
	from Final_CC as a left join ff as b
	on a.date=b.date1;
quit;


proc sql;
	create table Final_OC as
	select distinct a.*, b.*
	from Final_OC as a left join ff as b
	on a.date=b.date1;
quit;

proc sql;
	create table Final_CO as
	select distinct a.*, b.*
	from Final_CO as a left join ff as b
	on a.date=b.date1;
quit;



*************************************************************************************
					STEP-7: Run Fama-French factor models
************************************************************************************;

***Alpha;
proc sort data=Final_CC; by AnomalyVar Rank_Anomaly; run;
proc reg data=Final_CC noprint tableout outest=Alpha_CC;
  by AnomalyVar Rank_Anomaly;
  model VWHoldRet = MKT_RF smb hml;
  model VWHoldRet = MKT_RF smb hml cma rmw;
  model EWHoldRet = MKT_RF smb hml;
  model EWHoldRet = MKT_RF smb hml cma rmw;
quit;

data Alpha_CC;
  set Alpha_CC;
  where _Type_ in ('PARMS','T');
  keep AnomalyVar Rank_Anomaly _Type_ Intercept MKT_RF smb hml cma rmw _DEPVAR_;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;



proc sort data=Final_CO; by AnomalyVar Rank_Anomaly; run;
proc reg data=Final_CO noprint tableout outest=Alpha_CO;
  by AnomalyVar Rank_Anomaly;
  model VWHoldRet = MKT_RF smb hml;
  model VWHoldRet = MKT_RF smb hml cma rmw;
  model EWHoldRet = MKT_RF smb hml;
  model EWHoldRet = MKT_RF smb hml cma rmw;
quit;

data Alpha_CO;
  set Alpha_CO;
  where _Type_ in ('PARMS','T');
  keep AnomalyVar Rank_Anomaly _Type_ Intercept MKT_RF smb hml cma rmw _DEPVAR_;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;



proc sort data=Final_OC; by AnomalyVar Rank_Anomaly; run;
proc reg data=Final_OC noprint tableout outest=Alpha_OC;
  by AnomalyVar Rank_Anomaly;
  model VWHoldRet = MKT_RF smb hml;
  model VWHoldRet = MKT_RF smb hml cma rmw;
  model EWHoldRet = MKT_RF smb hml;
  model EWHoldRet = MKT_RF smb hml cma rmw;
quit;

data Alpha_OC;
  set Alpha_OC;
  where _Type_ in ('PARMS','T');
  keep AnomalyVar Rank_Anomaly _Type_ Intercept MKT_RF smb hml cma rmw _DEPVAR_;
  if _Type_='PARMS' then Intercept=Intercept;
  rename Intercept=Alpha;
  rename _Type_ =Stat;
run;




/*Export method-1 CSVs;
proc export data=Alpha_CC outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Alpha_CC.csv" 
dbms=csv replace;
run;

proc export data=Alpha_CO outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Alpha_CO.csv" 
dbms=csv replace;
run;

proc export data=Alpha_OC outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Alpha_OC.csv" 
dbms=csv replace;
run; */


*Export method-2 CSVs;
proc export data=Final_CC2 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_CC2.csv" 
dbms=csv replace;
run;

proc export data=Final_CO2 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_CO2.csv" 
dbms=csv replace;
run;

proc export data=Final_OC2 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_OC2.csv" 
dbms=csv replace;
run;

*Export method-3 CSVs;
proc export data=Final_CC3 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_CC3.csv" 
dbms=csv replace;
run;

proc export data=Final_CO3 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_CO3.csv" 
dbms=csv replace;
run;

proc export data=Final_OC3 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Final_OC3.csv" 
dbms=csv replace;
run;


/*proc sort data=final_cc nodupkey out=test; by AnomalyVar; run;

proc sql;
	create table test1 as
	select distinct *
	from variables
	where variable not in (select AnomalyVar from test);
quit;*/

proc sql;
	drop table Final_CC, Final_CC2, Final_CC3, Final_CO, Final_CO2, Final_CO3, Final_OC, Final_OC2, Final_OC3;
quit;


proc export data=social4 outfile="C:\Users\KUTHURU\Desktop\data.csv" 
dbms=csv replace;
run;
