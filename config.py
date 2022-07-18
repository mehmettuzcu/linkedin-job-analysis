from sqlalchemy import create_engine
import sqlalchemy

################## Connecting string ##################
engine = create_engine(f'postgresql://root:root@localhost:5432/test')
engine.connect()


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
jobs = ['engineer', 'scientist', 'analyst']
country = [['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102105699,locationFallback-%3ETurkey', 'Turkey'],
           ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A103644278,locationFallback-%3EUnited%20States', 'USA'],
           ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A101282230,locationFallback-%3EGermany', 'Germany']]

# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A105117694,locationFallback-%3ESweden', 'Sweden']
# ['geoUrn-%3Eurn%3Ali%3Afs_geo%3A102890719,locationFallback-%3ENetherlands', 'Netherlands']
################## Cookies and Headers ##################


cookies = {

    'JSESSIONID': '"ajax:0872958190215893462"',
    'li_at': 'AQEDASq5JWoDJalFAAABgfJOvZ8AAAGCFltBn00AVJc-6mNzmn-3QZm4dwHtlPB40vOZqiQJXkCjv-8f-aa3MQ0QOryBotqPTM_3L46A0_P7ADYPkOuqL63pWkwwoh-fO3b898Z7aiRHFWTUPDNj1Sqq',
}

headers = {
    'csrf-token': 'ajax:0872958190215893462',
    'x-restli-protocol-version': '2.0.0',
}

headers2 = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'csrf-token': 'ajax:0872958190215893462'
}
