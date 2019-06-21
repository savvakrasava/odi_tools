#-*- coding: utf-8 -*- 
'''
Created on 21 06 2019 

@author: lukhnevsn
'''



#import packages
##############################################

from java.io import BufferedReader ,FileReader, IOException;
from java.sql import PreparedStatement,Connection,DriverManager,SQLException,Statement,Types, ResultSet;
from java.net import Proxy, InetSocketAddress;
from okhttp3 import Call, OkHttpClient, Request, Response;
from okhttp3.OkHttpClient import newBuilder;
from okhttp3.Request import newBuilder as Builder;
from org.apache.http.impl.client import BasicCredentialsProvider, CloseableHttpClient, HttpClients;
from org.apache.http import  HttpEntity,HttpHost;
from org.apache.http.auth import AuthScope, NTCredentials;
from jarray import zeros; 
from au.com.bytecode.opencsv import CSVReader; 
from java.lang.String import  length, replace;

#import org.apache.commons.vfs2.util.Cryptor as Cryptor;
#import org.apache.commons.vfs2.util.CryptorFactory as CryptorFactory;

import okhttp3.Credentials as Credentials;
import java.io.File as File;
import java.io.FileOutputStream as FileOutputStream;
import java.io.InputStreamReader as InputStreamReader;
import java.io.FileInputStream as FileInputStream;
import java.sql.ResultSet;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream as BZip2CompressorInputStream;
import java.nio.file.Files as Files;
import java.nio.file.Paths as Paths;
import java.nio.file.StandardCopyOption as StandardCopyOption;
import java.util.concurrent.TimeUnit as TimeUnit;
import java.lang;
import org.apache.http.client.config.RequestConfig as RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse as CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet as HttpGet;
import java.sql.Timestamp as Timestamp; 
import java.lang.System as System; 
import java.sql.Timestamp as Timestamp;





#constants
##############################################

archived_file = 'list_of_expired_passports.csv.bz2'; #filname for downloading file
unarchived_file = 'list_of_expired_passports.csv'; #filname for uncompressed file
url = "http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2" ; #url = 'https://xn--b1ab2a0a.xn--b1aew.xn--p1ai/upload/expired-passports/list_of_expired_passports.csv.bz2'; #url = 'http://localhost/pass/list_of_expired_passports.csv.bin';
ODIAgent = '<%=odiRef.getSession( "AGENT_NAME" )%>';
ora_odi_pass_e = "BD15AFA077FA694DB138CD75D5BD19BA"; 
ora_odi_pass = CryptorFactory.getCryptor().decrypt(ora_odi_pass_e);
conn = odiRef.getJDBCConnection( "DEST" );
s_schema = '<%=odiRef.getInfo("DEST_SCHEMA")%>'
table_name = 'DIM_INVALID_PASSPORTS';
batch_size = 500000; #batch for inserting
buffer_size = 1024*8; #buffer for reading from web
downloaded = 0; #help const
lnNum =0; #help const
lastPercent = -1; #help const
totalRecords= 0; #help const


#functions
##############################################



def ins_log(state, err_text) :
    '''
    ANALSYS PROCCESSES LOGGING FUNC
    '''
    global conn;
    conn.setSchema(s_schema);
    cLog = conn.prepareCall("{call P_INS_LOG(DIM_INVALID_PASSPORTS,?,?)}");
    cLog.setString("sSTATE",state);
    if (err_text != None) :
        cLog.setString("sERRTEXT", err_text);
    else :
        cLog.setNull("sERRTEXT",Types.VARCHAR);
    cLog.execute();
    cLog.close();
    
def print_download_process(l1, l2): 
    '''
    FUNC FOR PRINT DOWNLOAD PERCENTS
    '''
    global lastPercent;
    currentPercent = (l1*100)/l2;        
    if(lastPercent < currentPercent): 
        print('='*currentPercent);
        print('\n current = '+str(l1)) 
        print('\n target = ' +str(l2))      
        lastPercent = currentPercent;        
        
def decompress_file(archived_file, unarchived_file):
    '''
    FUNC TO DECOMPRESS BZIP2 ARCHIVE
    '''
    ins_log("Decompressing...", str(archived_file));
    try:
        gzis = BZip2CompressorInputStream(FileInputStream(archived_file));
        Files.copy(gzis, Paths.get(unarchived_file), StandardCopyOption.REPLACE_EXISTING);
    except Exception:
        ins_log('Decompressing error', str(Exception));
    ins_log("File Decompressed!", str(unarchived_file));
        
def proxy_authenticator():
    '''
    FUNC FOR PROXY THIS AUTH.
    '''
    global proxy_user, proxy_pass;
    credential = Credentials.basic(proxy_user, proxy_pass);
    return Response.request().newBuilder().header("Proxy-Authorization", credential).build();    

       
def download_file():   
    '''
    FUNC FOR DOWNLOADING DATA FROM MVD.RF WEBSITE
    '''  
    global buffer_size, downloaded;
    ins_log('Create proxy settings', 'using '+str(ODIAgent));
    if ODIAgent == 'Internal':
        proxy_address = TEST_FILE_DOWNLOADS.ProdProxyHost;
        proxy_port    = TEST_FILE_DOWNLOADS.ProdProxyPort;
        proxy_user = 'lukhnevsn';
        proxy_passe   = "70320DB646F3C6740262E9224E8A88C7"; 
        proxy_domain  = "BANKEXP";     
        proxy_pass    = cryptor.decrypt(proxy_passe);
    else:
        proxy_address = FILE_DOWNLOADS.ProdProxyHost;
        proxy_port    = FILE_DOWNLOADS.ProdProxyPort;
        proxy_user    = "ODI_USER";
        proxy_passe   = "32A47DEE17B2F967BA6094BB609ABF8E"; 
        proxy_domain  = "BANKEXP";
        proxy_pass    = cryptor.decrypt(PROXY_PASSE);
    ins_log("Downloading...", url); 
    builder = OkHttpClient.Builder();    #builder.followRedirects(False).followSslRedirects(False);
    builder.connectTimeout(5, TimeUnit.MINUTES).writeTimeout(5, TimeUnit.MINUTES).readTimeout(5, TimeUnit.MINUTES); 
    httpClient = builder.proxy(Proxy( Proxy.Type.HTTP, InetSocketAddress(proxy_address, proxy_port))).proxyAuthenticator(proxy_authenticator).build();
    call = httpClient.newCall(Request.Builder().url(url).get().build()); #//Call to server
    response = call.execute(); #//
    ins_log('Call to web server', str(response));
    #print(response.code())
    if (response.code() == 200):  #//Check Response code
        inputStream = None;
        outputStream = None;           
        target = response.body().contentLength();  
        try:
            inputStream = response.body().byteStream(); #//Get stream of bytes
            buffer =  zeros(buffer_size, 'b'); #//Creating buffer bytes(1024*4)  #bytearray(4096)          
            outputStream =  FileOutputStream(File(archived_file));
            print_download_process(0, target);
            while (downloaded < target) :  
                readed = inputStream.read(buffer);       
                if (readed == -1):
                    break;
                else:
                    outputStream.write(buffer, 0, readed);                    #//write buff
                    downloaded += readed;                 
                    print_download_process(downloaded, target);                            
        except Exception:
            ins_log("Downloading Error", str(Exception));            
        finally:
            if (inputStream != None):
                inputStream.close();               
            elif(outputStream != None):
                outputStream.close();  
      
          
        ins_log("File downloaded!", str(url) + ' filename:'+str(archived_file));    
        #print("File downloaded! "+ str( url));

        
def truncate_table():
    
    global conn, table_name;
    conn.setSchema(s_schema);
    ins_log('Truncate table', str(s_schema)+'.'+str(table_name));
    try: 
        sql = "{call reference_editor.truncate_table(?)}";        
        print(sql);
        statement = conn.prepareCall(sql);
        statement.setString(1,table_name);
        statement.execute();  
        statement.close();        
    except SQLException as e:
        ins_log('Truncate error', str(e.getMessage())) ; 
        #print(e.getMessage());
    finally:
        #conn.close(); #//Close connection to DB
        ins_log("Table truncated ", str(s_schema)+'.'+str(table_name));
        
def replace_str(char):
    if  char == None: 
        return 'NULL';
    elif length(char) < 1:
        return 'NULL';        
    else:
        return char;
        
def insert_file_data_to_db_batch():
    '''
    FUNC FOR DATA INSERTING INTO DATABASE
    '''
    ins_log("Adding data to DB...", 'Using JDBC from File');    
    global conn, unarchived_file, lnNum, totalRecords, batch_size;
    conn.setSchema(s_schema);
    truncate_table();
    conn.setAutoCommit(False); 
    #jdbc_insert_sql = 'insert into dim_invalid_passports (pass_serial, pass_no, valid_to_dttm) values (?,?,?)';
    jdbc_insert_sql  = 'insert into dim_invalid_passports (pass_serial, pass_no, sys_actual_flg, sys_deleted_flg, valid_from_dttm,  valid_to_dttm, sys_mod_by) values (?, ?, ?, ?, ?, ?, ? )';
    #print(jdbc_insert_sql);
    sql_statement = conn.prepareStatement(jdbc_insert_sql);      
    reader = CSVReader(FileReader(unarchived_file));#/* Read CSV file in OpenCSV */
    nextLine = reader.readNext();   
    while reader.readNext() != None:
        lnNum+=1;
        sql_statement.setString(1, replace_str(reader.readNext()[0]));
        sql_statement.setString(2, replace_str(reader.readNext()[1]));
        sql_statement.setInt(3, 1);
        sql_statement.setInt(4, 0);
        sql_statement.setTimestamp(5, Timestamp(System.currentTimeMillis()));
        sql_statement.setString(6, "01.01.2400");
        sql_statement.setString(7, "ODI_USER");
        sql_statement.addBatch();  
        if lnNum%batch_size==0 or reader.readNext()==None:
            sql_statement.executeBatch();  
            #print(sql_statement.executeBatch());
            conn.commit();                          
    sql_statement.close();
    ins_log('File inserted, '+str(lnNum)+" rows added;", str(jdbc_insert_sql) + ' batch_size = '+str(batch_size)); 
    conn.close(); #//Close connection to DB
           


def do_it():
    '''
    EASY CALL TO OUR PROCESS
    '''
    ins_log('Begin', None);
    download_file();
    decompress_file(archived_file, unarchived_file);
    insert_file_data_to_db_batch();
    ins_log('Done', None);
    conn.close();


#main
##############################################
do_it();

