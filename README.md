# contracts_accounting
 
This is a small program for the organization of accounting for government contracts.
Conditionally divided into several modules:
1. Module for uploading information to the database
This module is responsible for working with various sources of information, currently implemented interaction with EXCEL, WORD, XML
Information from sources is loaded, cleaned up using various methods, and transmitted to the POSTGRESQL database using a simple ORM
2. Module for API formation
The module is responsible for receiving API requests, extracting the necessary information from the database, and transmitting the corresponding responses via the API
3. Frontend - module for displaying HTML pages
