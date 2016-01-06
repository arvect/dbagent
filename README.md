# dbagent
## A very light-weight Database agent ORM

Hibernate requires ORM mapping. ActiveJDBC requires instrumentation. This dbagent package is lighter and easier!

###Assume we create an class of Organzition like below,

public class Organization { 
&nbsp;&nbsp;    public String name;
&nbsp;&nbsp;    public String homepage;
&nbsp;&nbsp;    public double capital;

&nbsp;&nbsp;  static {
&nbsp;&nbsp;&nbsp;&nbsp;        String TABLENAME = "Organization";
&nbsp;&nbsp;&nbsp;&nbsp;        String[] TABLEKEY = {"name"};
&nbsp;&nbsp;&nbsp;&nbsp;        HashMap<String, Integer> hm = new HashMap();
&nbsp;&nbsp;&nbsp;&nbsp;        hm.put("name", 20);
&nbsp;&nbsp;&nbsp;&nbsp;        hm.put("homepage", 50); //Optional
&nbsp;&nbsp;&nbsp;&nbsp;        dbagent = new MysqlAgent(Organization.class, TABLENAME, TABLEKEY, hm);
&nbsp;&nbsp;&nbsp;&nbsp;        dbagent.createTable(); //Optional and only create table if not created yet.
&nbsp;&nbsp;  }
&nbsp;&nbsp;  public Organization() { // A Constructor without parameter must be here! Of course other Constructor can be created.
&nbsp;&nbsp;  }
}  

We can instantiate an Organization and save it to database like this:

&nbsp;&nbsp;  Organization orga = new Organziation();<br/>
&nbsp;&nbsp;    orga.name = "antianything";<br/>
&nbsp;&nbsp;    orga.homepage = "blala.org";<br/>
&nbsp;&nbsp;    orga.capital = 3000;<br/>
&nbsp;&nbsp;    Organization.dbagent.replaceToDB();<br/>

Now we select from mysql database,
&nbsp;&nbsp;    antianything    &nbsp;&nbsp;&nbsp;&nbsp; blala.org   &nbsp;&nbsp;&nbsp;&nbsp; 30000

Currently only MySQL agent is implemented.
