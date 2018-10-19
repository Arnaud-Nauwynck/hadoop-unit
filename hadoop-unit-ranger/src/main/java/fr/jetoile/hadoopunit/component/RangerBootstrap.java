package fr.jetoile.hadoopunit.component;

import java.net.URL;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.server.tomcat.EmbeddedServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;

public class RangerBootstrap implements Bootstrap {
	
    final public static String NAME = Component.RANGER.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(RangerBootstrap.class);

    public static final String RANGER_PORT = "ranger.port";

    private State state = State.STOPPED;

    private Configuration configuration;
    private String host;
    private int port;
    private String rangerJpaJdbcUrl;
    private String rangerJpaJdbcUser;
    private String rangerJpaJdbcPassword;

    private EmbeddedServer rangerServer;
    
    public RangerBootstrap() {
        this(null);
    }

    public RangerBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }
    
	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getProperties() {
		return "\n \t\t\t port:" + port;
	}

	private void loadConfig() throws BootstrapException {
        host = configuration.getString(HadoopUnitConfig.RANGER_HOST_KEY, "localhost");
        port = configuration.getInt(HadoopUnitConfig.RANGER_PORT_KEY, 6080);
        rangerJpaJdbcUrl = configuration.getString(HadoopUnitConfig.RANGER_JPA_JDBC_URL_KEY, "jdbc:log4jdbc:mysql://localhost/ranger");
        rangerJpaJdbcUser = configuration.getString(HadoopUnitConfig.RANGER_JPA_JDBC_USER_KEY, "rangeradmin");
        rangerJpaJdbcPassword = configuration.getString(HadoopUnitConfig.RANGER_JPA_JDBC_PASSWORD_KEY, "rangeradmin");
	}
	
	@Override
	public void loadConfig(Map<String, String> configs) {
		if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.RANGER_HOST_KEY))) {
            host = configs.get(HadoopUnitConfig.RANGER_HOST_KEY);
        }
		if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.RANGER_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.RANGER_PORT_KEY));
        }
		if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_URL_KEY))) {
			rangerJpaJdbcUrl = configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_URL_KEY);
		}
		if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_USER_KEY))) {
			rangerJpaJdbcUser = configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_USER_KEY);
		}
		if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_PASSWORD_KEY))) {
			rangerJpaJdbcPassword = configs.get(HadoopUnitConfig.RANGER_JPA_JDBC_PASSWORD_KEY);
		}
	}

	@Override
	public Bootstrap start() {
		if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
            	String configFileName = "ranger-admin-default-site.xml";
            	rangerServer = new EmbeddedServer(new String[] { configFileName });
            	
            	// TODO
            	
            	state = State.STARTED;
            	LOGGER.info("{} is started", this.getClass().getName());
            } catch (Exception e) {
            	state = State.STOPPED;
                LOGGER.error("unable to bootstrap ranger", e);
            }
        }
		return this;
	}

	@Override
	public Bootstrap stop() {
		if (state == State.STARTED) {
            state = State.STOPPING;
            LOGGER.info("{} is stopping", this.getClass().getName());
            if (rangerServer != null) {
	            try {
	            	rangerServer.shutdownServer();
	            } catch(Exception ex ) {
	            	LOGGER.error("unable to shutdown ranger", ex);
	            }
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
		return this;
	}
    
}
