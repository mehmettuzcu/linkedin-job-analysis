from sqlalchemy import create_engine
import sqlalchemy

################## Connecting string ##################
engine = create_engine(f'postgresql://root:root@localhost:5432/test')

creds = {'user': 'tuzcu',
            'passwd': 'mehmet123',
            'host': '54.93.103.1',
            'port': 3306,
            'db': 'test'}

# MySQL conection string.
connstr = 'mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}'

mysql_conn = create_engine(connstr.format(**creds))


database_username = 'tuzcu'
database_password = 'mehmet123'
database_ip='54.93.103.1'
database_name='test'
database_port = '3306'

def sqlcol(dfparam):
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):

        if i == "description_text":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if i != "description_text" and "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=255)})

        if i == "jobPostingUrl":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if i == "companyName":
            dtypedict.update({i: sqlalchemy.types.Text()})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATE()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.BIGINT()})

    return dtypedict

################## Jobs and Country Informations ##################
jobs = ['engineer', 'scientist']

country = [['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102105699,locationFallback-%3ETurkey', 'Turkey'],
           ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A103644278,locationFallback-%3EUnited%20States', 'USA']]

# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A101282230,locationFallback-%3EGermany', 'Germany']
# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A105117694,locationFallback-%3ESweden', 'Sweden']
# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102890719,locationFallback-%3ENetherlands', 'Netherlands']
################## Cookies and Headers ##################



cookies = {
    'JSESSIONID': '"ajax:8174042621654042084"',
    'li_at': 'AQEDASq5JWoF56CdAAABgjWlbUgAAAGCWbHxSE0AsS5kxVnWlV0l91m4kvxJQpuaS-uXCenQcM1NDi8jI0y53Dk4fPFMRvY907LeBYkqKa1D_wi7OFwwxMCKSHM-qJbC_eV3YHkCFD6uK5YQP2cvK1k2',

}

headers = {'csrf-token': 'ajax:8174042621654042084', 'x-restli-protocol-version': '2.0.0',
}

cookies2 = {
    'li_at': 'AQEDASq5JWoF56CdAAABgjWlbUgAAAGCWbHxSE0AsS5kxVnWlV0l91m4kvxJQpuaS-uXCenQcM1NDi8jI0y53Dk4fPFMRvY907LeBYkqKa1D_wi7OFwwxMCKSHM-qJbC_eV3YHkCFD6uK5YQP2cvK1k2',
    'JSESSIONID': '"ajax:8174042621654042084"'
}

headers2 = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'csrf-token': 'ajax:8174042621654042084'
}
