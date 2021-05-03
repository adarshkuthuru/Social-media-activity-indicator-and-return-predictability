*ssc install xtfmb
*ssc install outreg2
*ssc install estout

clear
set more 1
*capture log close

cd "C:\Users\KUTHURU\Desktop"
import delimited "C:\Users\KUTHURU\Desktop\data.csv", clear
save adarsh_data, replace 

*log using adarshdata.txt, replace
use adarsh_data.dta
*drop if (date != "2020-10-07")
*ssc install asreg
eststo clear

*ssc install asreg
/*regressions */

quietly xi: areg ret1 sum_var2, a(date)/* Model 1*/
quietly eststo m1

reg ret1 sum_var2, vce(robust)
quietly eststo m2
reg ret1 sum_var2, a(date) vce(robust)
quietly eststo m3

esttab using stats.csv, replace compress t nogaps b(%8.3f) drop(_cons) 
order(ret1 sum_var2)
 





clear 

use adarsh_data.dta


bys date: asreg ret1 sum_var2, se

/* To calculate the t-stats from each regression */
gen t_stats =  _b_sum_var2  /  _se_sum_var2

collapse _b_sum_var2 t_stats, by(date)

list date _b_sum_var2 t_stats

/*Average of _b_sum_var1 across dates */
collapse _b_sum_var2
di _b_sum_var2

/*Using xtfmb command to check the results */

/* Since date and ric are string variable, I would de-string it */
clear 

use adarsh_data.dta

encode date, gen(time)
encode ric, gen(ric1)

xtset ric1 time
*ssc install xtfmb

xtfmb ret1 sum_var2

 
 
 
 
 
 
 
 
 
 
 
 
 