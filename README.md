# dbagent
Database agent ORM

Hibernate requires ORM mapping. ActiveJDBC requires instrumentation. This dbagent package is lighter and easier!

Assume we create an class of Organzition like below,

public class Organization {
    public String name;
    public String homepage;
    public double capital;

  static {
        String TABLENAME = "Organization";
        String[] TABLEKEY = {"name"};
        HashMap<String, Integer> hm = new HashMap();
        hm.put("name", 20);
        hm.put("homepage", 50); //Optional
        dbagent = new MysqlAgent(Organization.class, TABLENAME, TABLEKEY, hm);
        dbagent.createTable(); //Optional and only create table if not created yet.
  } 
  public Organization() { // A Constructor without parameter must be here! Of course other Constructor can be created.
  }

We can instantiate an Organization and save it to database like this:
    Organization orga = new Organziation();
    orga.name = "antianything";
    orga.homepage = "blala.org";
    orga.capital = 3000;
    Organization.dbagent.replaceToDB();
    
Now we select from mysql database, 
    antianything            blala.org                  3000
    
Currently only MySQL agent is implemented. 
