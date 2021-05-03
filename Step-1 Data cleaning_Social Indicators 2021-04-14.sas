
proc datasets lib=work kill nolist memtype=data;
quit;

libname RA 'C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social'; run;




***************************************************************************************************************************************************************************
***************************************************************************************************************************************************************************

																	PART-1: DATA CLEANING

***************************************************************************************************************************************************************************
**************************************************************************************************************************************************************************;

**Import tsv files;
PROC IMPORT OUT= Identifiers
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\Ticker_Identifiers_2020-10-01_2021-03-31.tsv" 
            DBMS=TAB REPLACE;
RUN;
*2990 obs;


**Comment out;
/*data WORK.CC    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
   /*   infile 'C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\SocialIndicators_BroadUS_CloseToClose_2020-10-01_2021-03-31.tsv' delimiter='09'x
  MISSOVER DSD lrecl=32767 firstobs=2 ;
         informat Date yymmdd10. ;
         informat Start_Date anydtdte16.; 
         informat End_Date anydtdte16.;
         informat Ticker $4. ;
         informat Social_Relative_Intensity best32. ;
         informat Total_Social_Item_Count best32. ;
         informat Social_Intensity best32. ;
         informat Submission_Count best32. ;
         informat Comment_Count best32. ;
         informat Social_Sentiment $13. ;
         informat Financial_Sentiment best32. ;
         informat Non_Financial_Sentiment best32. ;
         informat Social_Interaction best32. ;
         informat Squeeze_Intensity__Text_ best32. ;
         informat YOLO_Intensity__Text_ best32. ;
         informat Rocket_Intensity__Text_ best32. ;
         informat Moon_Intensity__Text_ best32. ;
         informat Buy_Intensity__Text_ best32. ;
         informat Sell_Intensity__Text_ best32. ;
         informat Hold_Intensity__Text_ best32. ;
         informat Short_Intensity__Text_ best32. ;
         informat Crash_Intensity__Text_ best32. ;
         informat Bag_Holder_Intensity__Text_ best32. ;
         informat Dump_Intensity__Text_ best32. ;
         informat Lost_Intensity__Text_ best32. ;
         informat Drop_Intensity__Text_ best32. ;
         informat _10_Bagger_Intensity__Text_ best32. ;
         informat Gamma_Squeeze_Intensity__Text_ best32. ;
         informat Debt_Intensity__Text_ best32. ;
         informat Options_Intensity__Text_ best32. ;
         informat Put_Options_Intensity__Text_ best32. ;
         informat Call_Options_Intensity__Text_ best32. ;
         informat Implied_Volatility_Intensity__Te best32. ;
         informat Volatility_Intensity__Text_ best32. ;
         informat Diamond_Intensity__Emoji_ best32. ;
         informat Rocket_Intensity__Emoji_ best32. ;
         informat Gain_Intensity__Flair_ $12. ;
         informat Loss_Intensity__Flair_ $12. ;
         informat DD_Intensity__Flair_ $12. ;
         informat YOLO_Intensity__Flair_ $12. ;
         informat Options_Intensity__Flair_ $12. ;
         informat Discussion_Intensity__Flair_ $12. ;
         informat News_Intensity__Flair_ $12. ;
         informat PNL__Flair_ $12. ;

         format Date yymmdd10. ;
         format Start_Date datetime16. ;
         format End_Date datetime16. ;
         format Ticker $4. ;
         format Social_Relative_Intensity best32. ;
         format Total_Social_Item_Count best32. ;
         format Social_Intensity best32. ;
         format Submission_Count best32. ;
         format Comment_Count best32. ;
         format Social_Sentiment $13. ;
         format Financial_Sentiment best32. ;
         format Non_Financial_Sentiment best32. ;
         format Social_Interaction best32. ;
         format Squeeze_Intensity__Text_ best32. ;
         format YOLO_Intensity__Text_ best32. ;
         format Rocket_Intensity__Text_ best32. ;
         format Moon_Intensity__Text_ best32. ;
         format Buy_Intensity__Text_ best32. ;
         format Sell_Intensity__Text_ best32. ;
         format Hold_Intensity__Text_ best32. ;
         format Short_Intensity__Text_ best32. ;
         format Crash_Intensity__Text_ best32. ;
         format Bag_Holder_Intensity__Text_ best32. ;
         format Dump_Intensity__Text_ best32. ;
         format Lost_Intensity__Text_ best32. ;
         format Drop_Intensity__Text_ best32. ;
         format _10_Bagger_Intensity__Text_ best32. ;
         format Gamma_Squeeze_Intensity__Text_ best32. ;
         format Debt_Intensity__Text_ best32. ;
         format Options_Intensity__Text_ best32. ;
         format Put_Options_Intensity__Text_ best32. ;
         format Call_Options_Intensity__Text_ best32. ;
         format Implied_Volatility_Intensity__Te best32. ;
         format Volatility_Intensity__Text_ best32. ;
         format Diamond_Intensity__Emoji_ best32. ;
         format Rocket_Intensity__Emoji_ best32. ;
         format Gain_Intensity__Flair_ $12. ;
         format Loss_Intensity__Flair_ $12. ;
         format DD_Intensity__Flair_ $12. ;
         format YOLO_Intensity__Flair_ $12. ;
         format Options_Intensity__Flair_ $12. ;
         format Discussion_Intensity__Flair_ $12. ;
         format News_Intensity__Flair_ $12. ;
         format PNL__Flair_ $12. ;

input
          Date yymmdd10. 
          Start_Date datetime16.
          End_Date datetime16. 
          Ticker $4. 
          Social_Relative_Intensity best32. 
          Total_Social_Item_Count best32. 
          Social_Intensity best32. 
          Submission_Count best32. 
          Comment_Count best32. 
          Social_Sentiment $13. 
          Financial_Sentiment best32. 
          Non_Financial_Sentiment best32. 
          Social_Interaction best32. 
          Squeeze_Intensity__Text_ best32. 
          YOLO_Intensity__Text_ best32. 
          Rocket_Intensity__Text_ best32. 
          Moon_Intensity__Text_ best32. 
          Buy_Intensity__Text_ best32. 
          Sell_Intensity__Text_ best32. 
          Hold_Intensity__Text_ best32. 
          Short_Intensity__Text_ best32. 
          Crash_Intensity__Text_ best32. 
          Bag_Holder_Intensity__Text_ best32. 
          Dump_Intensity__Text_ best32. 
          Lost_Intensity__Text_ best32. 
          Drop_Intensity__Text_ best32. 
          _10_Bagger_Intensity__Text_ best32. 
          Gamma_Squeeze_Intensity__Text_ best32. 
          Debt_Intensity__Text_ best32. 
          Options_Intensity__Text_ best32. 
          Put_Options_Intensity__Text_ best32. 
          Call_Options_Intensity__Text_ best32. 
          Implied_Volatility_Intensity__Te best32. 
          Volatility_Intensity__Text_ best32. 
          Diamond_Intensity__Emoji_ best32. 
          Rocket_Intensity__Emoji_ best32. 
          Gain_Intensity__Flair_ $12. 
          Loss_Intensity__Flair_ $12. 
          DD_Intensity__Flair_ $12. 
          YOLO_Intensity__Flair_ $12. 
          Options_Intensity__Flair_ $12. 
          Discussion_Intensity__Flair_ $12. 
          News_Intensity__Flair_ $12. 
          PNL__Flair_ $12. 
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     /* run; 
*53449 obs; */

PROC IMPORT OUT= CC
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\SocialIndicators_BroadUS_CloseToClose_2020-10-01_2021-03-31.tsv" 
            DBMS=TAB REPLACE;
RUN;
*53449 obs;

data cc; 
	set cc; 
	rename Implied_Volatility_Intensity__Te=Implied_Volatility_Intensity;
	rename Gamma_Squeeze_Intensity__Text_=Gamma_Squeeze_Intensity;
	days=intck('days',Start_Date, End_Date);
run;



PROC IMPORT OUT= CO
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\SocialIndicators_BroadUS_CloseToOpen_2020-10-01_2021-03-31.tsv" 
            DBMS=TAB REPLACE;
RUN;
*41387 obs;

data co; 
	set co; 
	rename Implied_Volatility_Intensity__Te=Implied_Volatility_Intensity;
	rename Gamma_Squeeze_Intensity__Text_=Gamma_Squeeze_Intensity;
	days=intck('days',Start_Date, End_Date);
run;


PROC IMPORT OUT= OC
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\SocialIndicators_BroadUS_2020-10-01_2021-03-31\SocialIndicators_BroadUS_OpenToClose_2020-10-01_2021-03-31.tsv" 
            DBMS=TAB REPLACE;
RUN;
*31522 obs;

data oc; 
	set oc; 
	rename Implied_Volatility_Intensity__Te=Implied_Volatility_Intensity;
	rename Gamma_Squeeze_Intensity__Text_=Gamma_Squeeze_Intensity;
	days=intck('days',Start_Date, End_Date);
run;

**Sorting;
proc sort data=cc; by ticker date Start_Date; run;
proc sort data=co; by ticker date Start_Date; run;
proc sort data=oc; by ticker date Start_Date; run;

/**Renaming variables;
proc sql noprint ;
select cats(name,'=CC_',name) into :renames separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='CC'
  ;
quit;

proc datasets nolist library=work;
  modify CC;
    rename &renames;
  run;
quit;

proc sql noprint ;
select cats(name,'=CO_',name) into :renames separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='CO'
  ;
quit;

proc datasets nolist library=work;
  modify CO;
    rename &renames;
  run;
quit;

proc sql noprint ;
select cats(name,'=OC_',name) into :renames separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='OC'
  ;
quit;

proc datasets nolist library=work;
  modify OC;
    rename &renames;
  run;
quit; */


*************************************************************************************
			2. Add next day and next 7-days return variables to Russells datset
************************************************************************************;
**Import csv;
       data WORK.RUSSELL    ;
       %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
       infile 'C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\Russel3000_MarketData_2020___2020-08-01_2021-04-14__2021-04-14_112631.csv'
  delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
          informat Date mmddyy10. ;
          informat QuoteID $18. ;
          informat MktSymbol $4. ;
          informat RIC $7. ;
          informat ISIN $10. ;
          informat SEDOL $10. ;
          informat GICSIndustryCode best32. ;
          informat MktID $10. ;
          informat pr best32. ;
          informat ClosePrice best32. ;
          informat IssuerName $24. ;
          informat ExchangeDescription $23. ;
          informat ExchangeCode $3. ;
          informat MarketCapitalization best32. ;
          informat Volume best32. ;
          informat comment $20. ;
          format Date mmddyy10. ;
          format QuoteID $18. ;
          format MktSymbol $4. ;
          format RIC $7. ;
          format ISIN $10. ;
          format SEDOL $10. ;
          format GICSIndustryCode best12. ;
          format MktID $10. ;
          format pr best12. ;
          format ClosePrice best12. ;
          format IssuerName $24. ;
          format ExchangeDescription $23. ;
          format ExchangeCode $3. ;
          format MarketCapitalization best12. ;
          format Volume best12. ;
          format comment $20. ;
       input
                   Date
                   QuoteID  $
                   MktSymbol  $
                   RIC  $
                   ISIN  $
                   SEDOL  $
                   GICSIndustryCode
                   MktID  $
                   pr
                   ClosePrice
                   IssuerName  $
                   ExchangeDescription  $
                   ExchangeCode  $
                   MarketCapitalization
                   Volume
                   comment  $
       ;
       if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
       run;
*535958 obs;


*Step-4: Add next day and next 7-days return variables;
data russell;
	format date MMDDYY10. ric $7.  day 2. day1 YYMMDD10. day7 YYMMDD10.;
	set russell;
	Day=weekday(date);  *1 is Sunday, 4 is Wednesday, 7 is Saturday;
	Day1=intnx("day",Date,1);
	Day7=intnx("day",Date,7);
run;
*535958 obs;

data russell;
	set russell;
	nrow=_N_;
	if pr=-9999402 then pr=.;
	if ClosePrice=-9999402 then ClosePrice=.;
	if volume=-9999402 then volume=.;
	if MarketCapitalization=-9999402 then MarketCapitalization=.;
	if missing(closeprice)=1 or missing(MarketCapitalization)=1 or missing(volume)=1 then pr=.;
run;

*Add next 7-days return;
proc sql;
  create table russell2 as
  select distinct a.*, b.date as b_date, b.pr as b_pr, b.volume as b_volume, b.MarketCapitalization as b_MarketCapitalization
  from russell as a left join russell as b
  on a.ric=b.ric and a.Day1<=b.Date<a.Day7
  order by a.ric, a.Date;
quit;
*2,115,672 obs;

data russell2;
	set russell2;
	nrow=_N_;
run;

proc sql;
  create table russell3 as
  select distinct ric, Date, exp(sum(log(1+b_pr))) - 1 as ret7, mean(b_volume) as avg_volume, mean(b_MarketCapitalization) as avg_MarketCapitalization
  from russell2
  /*where a.ric=b.ric and a.CC_Start_Date<=b.CC_Date<=a.CC_End_Date and missing(a.CC_Start_Date)=0*/
  group by ric, Date;
quit;
*535958 obs;

*Add next day return;
proc sql;
  create table russell as
  select distinct a.*, b.date as date1, b.pr as ret1, b.volume as volume1, b.MarketCapitalization as MarketCapitalization1
  from russell as a left join russell as b
  on a.ric=b.ric and a.nrow=b.nrow-1
  order by a.ric, a.Date;
quit;
*535958 obs;

**Russell Final dataset;
proc sql;
	create table russell_final as
	select distinct a.*, b.day, b.ret1, b.volume1, b.MarketCapitalization
	from russell3 as a left join russell as b
	on a.date=b.date and a.ric=b.ric;
quit; 


*************************************************************************************
									2. MERGING - CC, CO
************************************************************************************;

*Step-1: Merge Identifiers with CC, CO (company-level social data merged with security-level identifiers data);
proc sql;
	create table cc as
	select distinct a.*, b.*
	from Identifiers as a left join cc as b
	on a.ticker=b.ticker
	order by a.ric, b.date;
quit;
*54006 obs;

/*data test; set cc1; if missing(cc_date)=1; run;*/
proc sql;
	create table co as
	select distinct a.*, b.*
	from Identifiers as a left join co as b
	on a.ticker=b.ticker
	order by a.ticker, b.date;
quit;
*42104 obs;

proc sql;
	create table oc as
	select distinct a.*, b.*
	from Identifiers as a left join oc as b
	on a.ticker=b.ticker
	order by a.ticker, b.date;
quit;
*32447 obs;

proc sql;
	drop table russell, russell2,  russell3;
quit;

/*Step-2: Merge CC, CO (security-level data);
proc sql;
	create table social as
	select distinct a.*, b.*
	from CC as a left join CO as b
	on a.ticker=b.ticker and a.ric=b.ric and a.CC_Date=b.CO_Date 
	order by a.ticker, a.ric, a.cc_date;
quit;
*54006 obs;

 proc sql;
	create table social as
	select distinct a.*, b.*
	from social as a left join OC as b
	on a.ticker=b.ticker and a.ric=b.ric and a.CC_Date=b.OC_Date 
	order by a.ticker, a.ric, a.cc_date;
quit; 
*54006 obs;

data social;
	format ticker isin ric CC_Date CO_Date CC_Start_Date CO_Start_Date CC_End_Date CO_End_Date CC_Days CO_Days;
	set social;
run;
proc sort data=social; by ticker ric CC_Date CC_Start_Date CC_End_Date; run; */

**# of Unique companies;
/* proc sort data=social nodupkey out=test1; by ticker; run;
*2990 obs;
/*data test1; set test1; keep ticker isin ric; run;*/

**# of Companies with no social data;
/*data test;
	set social;
	if missing(CC_Date)=1;
run;
*557 obs;

data test; set test; keep ticker isin ric; run;

*Export csv;
proc export data=Test outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 4\RA work\Social\NoData.csv" 
dbms=csv replace;
run;*/



*************************************************************************************
									2. MERGING - OC
************************************************************************************;
*Step-1: Merge Identifiers with OC (company-level social data merged with security-level identifiers data);


*Step-2: Merge Russell returns with social data;
/*proc sql;
  create table social_final2 as
  select distinct a.*, b.*
  from OC as a left join russell_final as b
  on a.ric=b.ric and a.OC_Date=b.Date and missing(a.OC_Date)=0
  order by a.ric, a.OC_Date, b.Date;
quit;
*32447 obs;


data ra.social_final1; set social_final; run;
data ra.social_final2; set social_final2; run; */



/**Avg weekly returns by stocks;
proc sql;
	create table test as
	select distinct ric, day, mean(ret7) as avg_ret
	from russell_final
	group by ric
	having day=4;
quit;

data test1;
	set test;
	if avg_ret>=0.2;
run;*/
