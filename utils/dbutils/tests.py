"""Test module for DBUtils"""
import unittest
import json
import psycopg2
from dbutils import DBUtils
from athenautils import AthenaUtils
from hiveutils import HiveUtils
from athenautils_async import AthenaUtilsAsync

DBCONFIG = """
{
    "db_host" : "db.staging.cloudchomp.local",
    "db_port" : "5432",
    "db_user" : "ccanroot",
    "db_pword": "ccand3v310per",
    "db_database": "ccanwarehouse",
    "db_appname" : "test_application"
}

"""

DBCONFIG_WRONGCRED = """
{
    "db_host" : "db.staging.cloudchomp.local",
    "db_port" : "5432",
    "db_user" : "ccan1root",
    "db_pword": "ccand3v310per",
    "db_database": "ccanwarehouse",
    "db_appname" : "test_application"
}

"""

HADOOP_CONFIG = """
{
"host": "ec2-34-211-124-115.us-west-2.compute.amazonaws.com",
"username": "hadoop",
"port": "10000",
"s3_workspace_bucket": "ccan-hadoop-lab",
"s3_output_bucket": "ccan-hadoop-lab-output"
}
"""

class TestDBUtils(unittest.TestCase):
    """Class for testing DBUtils"""

    @unittest.skip("Skip test")
    def test_connect_success(self):
        """Test to check validate successful connection"""
        with DBUtils(json.loads(DBCONFIG)) as dbobj:
            result = dbobj.execute_query('select version();')
            self.assertEqual(result[0].get('version'),
                             'PostgreSQL 9.5.4 on x86_64-pc-linux-gnu, ' +
                             'compiled by gcc (GCC) 4.8.2 20140120 ' +
                             '(Red Hat 4.8.2-16), 64-bit')

    @unittest.skip("Skip test")
    def test_connect_incorrect_creds(self):
        """Test to validate incorrect credentials"""
        with self.assertRaises(psycopg2.OperationalError):
            with DBUtils(json.loads(DBCONFIG_WRONGCRED)) as dbobj:
                pass
                
    @unittest.skip("Skip test")
    def test_athena_query(self):
        """Test to validate athena access"""
        with AthenaUtils() as dbobj:
            result = dbobj.execute_query("select * from sampledb.elb_logs")
            print("Query id: {0}".format(dbobj.query_id))
        print("Total number of rows: {0}".format(len(result.index)))
    #    print(result)

    def test_athena_async_query(self):
        """Test to validate athena access"""
        with AthenaUtilsAsync() as dbobj:
            query = """select sample_interval, """\
                    """ stat_group, stat_name, entity, avg(stat_value) average, """\
                    """ approx_percentile(stat_value, """\
                    """ARRAY[0,0.1,0.25,0.5,0.75,0.9,0.99,0.9999,1]) percentiles, """\
                    """ corr(stat_lag_0, stat_lag_5) acf_lag_5, corr(stat_lag_0, stat_lag_8) acf_lag_8, """\
                    """ corr(stat_lag_0, stat_lag_6) acf_lag_6, corr(stat_lag_0, stat_lag_12) acf_lag_12, """\
                    """ count_if(stat_value>200)*100/count(stat_value)gt_2_per,count_if(stat_value>500)*100/count(stat_value) gt_5_per, """\
                    """ count_if(stat_value>1000) *100/ count(stat_value)gt_10_per, """\
                    """ cast(array_agg(sample_time) AS JSON) sample_time_array, """\
                    """ cast(array_agg(stat_value) AS JSON) stat_value_array, """\
                    """ max_by(stat_value, sample_time) latest_value """\
                    """ from(select sample_time, sample_interval, stat_group, stat_name, entity,stat_value, """\
                    """ lag(stat_value,0,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_0, """\
                    """ lag(stat_value,1,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_1, """\
                    """ lag(stat_value,4,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_5, """\
                    """ lag(stat_value,7,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_8, """\
                    """ lag(stat_value,5,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_6, """\
                    """ lag(stat_value,11,0) over (partition by entity, stat_group, stat_name, sample_interval """\
                    """ order by sample_time asc) stat_lag_12 """\
                    """ from (select distinct sample_time, entity, sample_interval, stat_group, stat_name, stat_value """\
                    """ FROM customer_stats.p125899_ottvcs02 where ((stat_group = 'cpu' and stat_name = 'usage') or """\
                    """ (stat_group= 'mem' and stat_name= 'usage') or (stat_group= 'disk' and stat_name= 'provisioned') or """\
                    """ (stat_group= 'disk' and stat_name= 'maxTotalLatency') or """\
                    """ (stat_group= 'sys' and stat_name='uptime') or (stat_group = 'net' and stat_name= 'usage')) """\
                    """ and device_name='' and entity in ('vm-1632','vm-2706','imp-vm-408366','imp-vm-408362','vm-15759','vm-690','vm-15693','vm-1731','vm-1801','vm-653','vm-3128','vm-3355','vm-418','vm-334','vm-335','vm-336','vm-22234','vm-1890','vm-332','vm-4227','vm-867','vm-1626','vm-410','vm-5962','vm-25636','vm-17878','vm-18998','vm-28709','vm-338','vm-341','vm-344','vm-350','vm-355','vm-356','vm-359','vm-361','vm-363','vm-367','vm-386','vm-396','vm-404','vm-405','vm-407','vm-409','vm-414','vm-419','vm-428','vm-438','vm-442','vm-449','vm-464','vm-468','vm-470','vm-437','vm-446','vm-445','vm-431','vm-433','vm-423','vm-429','vm-461','vm-371','vm-456','vm-406','vm-413','vm-436','vm-435','vm-451','vm-453','vm-469','vm-420','vm-427','vm-430','vm-426','vm-458','vm-450','vm-425','vm-444','vm-374','vm-389','vm-392','vm-412','vm-434','vm-452','vm-385','vm-400','vm-424','vm-408','vm-462','vm-377','vm-439','vm-352','vm-394','vm-448','vm-443','vm-397','vm-415','vm-472','vm-473','vm-477','vm-481','vm-482','vm-488','vm-489','vm-490','vm-494','vm-498','vm-514','vm-515','vm-516','vm-636','vm-637','vm-638','vm-639','vm-640','vm-641','vm-642','vm-654','vm-655','vm-656','vm-657','vm-659','vm-660','vm-661','vm-662','vm-663','vm-667','vm-669','vm-670','vm-671','vm-680','vm-684','vm-686','vm-687','vm-688','vm-689','vm-697','vm-703','vm-705','vm-707','vm-787','vm-909','vm-917','vm-964','vm-981','vm-985','vm-1001','vm-1028','vm-487','vm-664','vm-480','vm-1041','vm-635','vm-685','vm-504','vm-503','vm-476','vm-587','vm-691','vm-508','vm-491','vm-471','vm-493','vm-698','vm-643','vm-634','vm-1052','vm-1067','vm-1165','vm-1171','vm-1254','vm-1263','vm-1282','vm-1343','vm-1404','vm-1482','vm-1504','vm-1524','vm-1624','vm-1628','vm-1630','vm-1631','vm-1637','vm-1638','vm-1642','vm-1644','vm-1648','vm-1652','vm-1658','vm-1662','vm-1665','vm-1680','vm-1681','vm-1683','vm-1688','vm-1700','vm-1701','vm-1708','vm-1712','vm-1719','vm-1725','vm-1726','vm-1730','vm-1738','vm-1740','vm-1746','vm-1748','vm-1750','vm-1751','vm-1758','vm-1759','vm-1762','vm-1765','vm-1771','vm-1772','vm-1800','vm-1804','vm-1805','vm-1042','vm-1690','vm-1752','vm-1747','vm-1706','vm-1636','vm-1707','vm-1054','vm-1691','vm-1692','vm-1627','vm-1696','vm-1056','vm-1724','vm-1699','vm-1295','vm-1454','vm-1807','vm-1809','vm-1813','vm-1815','vm-1818','vm-1824','vm-1825','vm-1827','vm-1828','vm-1829','vm-1830','vm-1831','vm-1833','vm-1839','vm-1843','vm-1847','vm-1849','vm-1850','vm-1855','vm-1857','vm-1859','vm-1862','vm-1864','vm-1873','vm-1874','vm-1878','vm-1880','vm-1882','vm-1884','vm-1895','vm-1896','vm-1897','vm-1903','vm-1906','vm-1907','vm-1910','vm-1914','vm-1921','vm-1922','vm-1925','vm-1927','vm-1929','vm-1931','vm-1933','vm-1945','vm-1948','vm-1949','vm-1950','vm-1954','vm-1955','vm-2001','vm-2020','vm-2030','vm-2031','vm-2058','vm-2067','vm-2070','vm-2077','vm-2121','vm-2139','vm-2169','vm-2236','vm-2467','vm-1851','vm-1888','vm-2039','vm-2122','vm-1881','vm-1861','vm-2526','vm-2603','vm-2605','vm-2695','vm-2741','vm-2857','vm-2873','vm-2882','vm-2937','vm-2939','vm-2940','vm-3018','vm-3036','vm-3049','vm-3050','vm-3092','vm-3127','vm-3132','vm-3135','vm-3136','vm-3145','vm-3152','vm-3197','vm-3198','vm-3199','vm-3200','vm-3203','vm-3208','vm-3215','vm-3217','vm-3221','vm-3277','vm-3280','vm-3303','vm-3330','vm-3331','vm-3332','vm-3334','vm-3342','vm-3345','vm-3346','vm-3349','vm-3352','vm-3354','vm-3357','vm-3394','vm-3395','vm-3425','vm-3437','vm-3458','vm-3460','vm-2637','vm-3333','vm-2533','vm-3390','vm-2636','vm-3185','vm-2540','vm-2477','vm-3344','vm-3121','vm-3336','vm-3392','vm-3340','vm-2638','vm-3335','vm-3341','vm-3505','vm-3639','vm-3726','vm-3856','vm-3863','vm-3905','vm-3910','vm-3920','vm-3935','vm-3972','vm-4025','vm-4061','vm-4062','vm-4177','vm-4253','vm-4265','vm-4694','vm-4758','vm-4974','vm-5073','vm-5122','vm-5191','vm-5195','vm-5221','vm-5248','vm-5289','vm-5459','vm-5496','vm-5606','vm-5607','vm-5608','vm-5693','vm-5721','vm-5808','vm-5818','vm-5985','vm-6015','vm-6016','vm-6044','vm-6078','vm-6158','vm-6206','vm-6207','vm-6046','vm-5975','vm-3719','vm-4704','vm-3465','vm-3577','vm-6003','vm-4005','vm-3903','vm-4877','vm-5512','vm-6208','vm-5758','vm-3919','vm-5487','vm-5977','vm-5196','vm-3672','vm-6054','vm-5724','vm-4693','vm-3904','vm-5628','vm-4977','vm-6247','vm-6399','vm-6570','vm-6645','vm-6674','vm-6677','vm-6701','vm-7321','vm-7757','vm-7768','vm-7775','vm-8109','vm-8290','vm-8654','vm-8905','vm-9052','vm-9064','vm-9440','vm-9459','vm-9838','vm-10486','vm-6618','vm-10633','vm-6615','vm-9063','vm-8284','vm-6492','vm-10663','vm-9431','vm-7762','vm-6780','vm-6241','vm-7540','vm-7552','vm-8800','vm-8289','vm-6598','vm-6607','vm-10667','vm-10592','vm-8659','vm-10652','vm-10259','vm-10230','vm-9435','vm-9438','vm-6385','vm-6725','vm-10630','vm-6303','vm-6495','vm-6493','vm-10277','vm-8032','vm-8684','vm-8265','vm-9419','vm-10274','vm-8030','vm-6330','vm-6465','vm-6636','vm-6662','vm-9895','vm-9902','vm-9516','vm-9430','vm-11449','vm-11462','vm-11916','vm-11920','vm-12339','vm-12341','vm-12359','vm-12382','vm-12415','vm-12847','vm-13110','vm-13313','vm-14048','vm-14526','vm-15243','vm-15756','vm-16944','vm-16970','vm-17273','vm-17852','vm-17860','vm-11919','vm-14218','vm-10818','vm-13737','vm-12904','vm-11071','vm-12333','vm-16698','vm-15762','vm-16088','vm-16132','vm-15746','vm-16645','vm-16178','vm-12311','vm-11280','vm-15698','vm-12365','vm-15235','vm-16204','vm-17776','vm-11069','vm-11451','vm-17312','vm-17775','vm-11085','vm-14506','vm-11279','vm-15230','vm-13160','vm-16658','vm-10895','vm-14234','vm-12853','vm-14214','vm-11923','vm-12252','vm-12275','vm-15757','vm-11440','vm-11910','vm-14211','vm-11460','vm-12355','vm-13353','vm-18176','vm-19292','vm-19337','vm-19407','vm-20635','vm-20656','vm-21393','vm-21449','vm-21934','vm-21950','vm-22240','vm-22834','vm-22838','vm-23197','vm-23911','vm-23963','vm-19743','vm-22231','vm-23958','vm-23389','vm-17875','vm-22236','vm-21893','vm-18986','vm-22251','vm-22184','vm-22076','vm-22233','vm-21461','vm-21476','vm-22031','vm-21932','vm-22225','vm-23960','vm-20104','vm-22071','vm-22087','vm-22230','vm-22238','vm-18947','vm-18950','vm-22084','vm-22032','vm-22078','vm-22085','vm-22101','vm-22221','vm-23930','vm-23847','vm-23941','vm-23959','vm-22228','vm-21462','vm-22754','vm-18280','vm-18961','vm-21963','vm-21068','vm-22222','vm-19942','vm-20654','vm-22068','vm-22063','vm-17899','vm-22080','vm-22227','vm-23965','vm-23981','vm-24189','vm-24597','vm-26023','vm-26034','vm-26127','vm-26129','vm-26161','vm-26503','vm-26504','vm-26707','vm-26727','vm-26742','vm-26744','vm-27287','vm-27356','vm-27515','vm-27596','vm-28102','vm-28666','vm-30543','vm-31033','vm-31038','vm-26895','vm-30028','vm-31053','vm-28173','vm-29325','vm-26112','vm-29851','vm-27483','vm-26179','vm-27406','vm-31039','vm-26032','vm-26033','vm-26025','vm-26030','vm-26031','vm-28174','vm-25091','vm-23964','vm-23979','vm-27582','vm-28175','vm-27538','vm-26109','vm-26111','vm-26029','vm-26708','vm-27872','vm-30303','vm-26024','vm-29392','vm-28172','vm-29331','vm-28705','vm-29299','vm-27520','vm-29360','vm-26028','vm-28720','vm-28765','vm-29852','vm-25126','vm-29312','vm-31538','vm-31935','vm-31965','vm-32110','vm-32902','vm-33020','vm-33553','vm-33571','vm-33586','vm-33931','vm-34043','vm-34057','vm-34064','vm-34116','vm-34554','vm-34555','vm-34560','vm-34571','vm-34589','vm-34591','vm-35572','vm-35579','vm-35601','vm-35917','vm-35918','vm-35919','vm-35920','vm-35922','vm-35923','vm-35943','vm-36063','vm-36069','vm-36000','vm-32402','vm-33630','vm-31064','vm-33554','vm-34192','vm-33573','vm-35573','vm-34132','vm-32476','vm-32563','vm-36043','vm-36042','vm-31567','vm-31065','vm-35038','vm-32603','vm-35357','vm-28732','vm-34559','vm-31067','vm-31891','vm-32601','vm-34081','vm-33061','vm-32564','vm-33550','vm-34098','vm-36067','vm-32602','vm-34925','vm-35571','vm-31066','vm-36348','vm-36390','vm-36408','vm-36505','vm-36533','vm-36915','vm-36918','vm-3358','vm-387','vm-34579','vm-34129','vm-9144','vm-22122','vm-1502','vm-33566','vm-35607','vm-1908','vm-8292','vm-33628','vm-32606','vm-6269','vm-32112','vm-36484','vm-22072','vm-22093','vm-22097','vm-22235','vm-26506','vm-15684','vm-6667','vm-6246','vm-32550','vm-6624','vm-36526','vm-36065','vm-36534','vm-35036','vm-28739','vm-22193','vm-13336','vm-22074','vm-18964','vm-6668','vm-16197','vm-14704','vm-36386','vm-36507','vm-555','vm-33611','vm-422','vm-21067','vm-12383','vm-1055','vm-6122','vm-4556','vm-22819','vm-28744','vm-30010','vm-362','vm-3124','vm-16965','vm-1860','vm-1742','vm-2487','vm-6223','vm-459','vm-15747','vm-6593','vm-26725','vm-28725','vm-1835','vm-28736','vm-35356','imp-vm-427068','imp-vm-427069','imp-vm-427076','imp-vm-427077',
'imp-vm-427078','vm-544','vm-577','vm-30529','vm-579','vm-10498','vm-557','vm-36388'))x) y"""\
                    """ group by entity, sample_interval, stat_name, stat_group """
            result = dbobj.execute_query_async(query)
            print(result)
          
        # print("Total number of rows: {0}".format(len(result)))

    @unittest.skip("Skip test")
    def test_athena_table_exists(self):
        """Test to check if table_exists function works"""
        with AthenaUtils() as dbobj:
            result = dbobj.check_table("customer_stats", "p125899_ottvcs02")

        self.assertEqual(result, True)

    def test_hadoop(self):
        """Test to check if we can query hive/hadoop"""
        with HiveUtils(json.loads(HADOOP_CONFIG)) as dbobj:
            result = dbobj.execute_query("create table test123456789(aa STRING)");
    
        self.assertEqual(True, True)


# TODO: Test select, insert, and update queries


# Run the tests
if __name__ == '__main__':
    unittest.main()
