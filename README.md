# Client data merger

Client data merger is a code prepared for merging and filtering client data (first dataset) with corresponding financial data (second dataset) for those clients. Code was prepared for loading files from **raw\_data\_files** directory (by default this will be done) and customer country filtering for **Netherlands** and **United Kingdom**. Then final file will be saved in **client\_data** as a **client\_data.csv**.

In the code there is  a list of columns that need to be loaded from both files

**client_data** - ['id', 'first\_name', 'last\_name', 'email', 'country']

**financial_data** - ['id', 'btc\_a', 'cc\_t', 'cc\_n'] 

Both datasets are merged on **id** column


## Usage

Program will default run with arguments written above (unless other ones are listed as below)

Code could be executed with custom arguments as shown:

	python __init__.py --cdn files/client.csv --fdn files/financialdata.csv --cl "Netherlands, United States" 

'--cdn' argument stands for client data name

'--fdn' argument stands for financial data name

'--cl' argument stands for countries list

