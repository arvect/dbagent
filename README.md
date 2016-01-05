# dbagent
<h2>A very light-weight Database agent ORM</H2>

<p>Hibernate requires ORM mapping. ActiveJDBC requires instrumentation. This dbagent package is lighter and easier!</p>

<H3>Assume we create an class of Organzition like below,</H3>

<code>public class Organization { <br/>
    public String name;<br/>
    public String homepage;<br/>
    public double capital;<br/>
<br/>
  static {<br/>
        String TABLENAME = "Organization";<br/>
        String[] TABLEKEY = {"name"};<br/>
        HashMap<String, Integer> hm = new HashMap();<br/>
        hm.put("name", 20);<br/>
        hm.put("homepage", 50); //Optional<br/>
        dbagent = new MysqlAgent(Organization.class, TABLENAME, TABLEKEY, hm);<br/>
        dbagent.createTable(); //Optional and only create table if not created yet.<br/>
  } <br/>
  public Organization() { // A Constructor without parameter must be here! Of course other Constructor can be created.<br/>
  }<br/>
</code>
<p>We can instantiate an Organization and save it to database like this:</p>
<code>
Organization orga = new Organziation();<br/>
    orga.name = "antianything";<br/>
    orga.homepage = "blala.org";<br/>
    orga.capital = 3000;<br/>
    Organization.dbagent.replaceToDB();<br/>
</code>    
<p>Now we select from mysql database, </p>
<code>    antianything            blala.org                  30000
</code>
<p>Currently only MySQL agent is implemented. </p>
