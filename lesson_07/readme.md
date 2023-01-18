Airflow learning. Windows + WSL

Method (4) was used

Methods:
(1) Airflow via docker installation
	Documentation: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html	
	Successfully:
		Installed containers
		Opened and logged in Web UI in browser
	Problems:
		Not solved: Can't connect to airflow Posgres DB
			Container found but i can't connect via Pycharm
		Not solved: Can't find ariflow SQLite DB
			Thas was in course
	Not checked:
		dag,logs,plugin folders was created in windows my project folder. But i not check that dags are accessible
	Conclusion:
		OK. Can be used with limitations.
	Not used finally

(2) Pycharm -> Try start airflow on windows. Remote Interpreter to airflow in docker
	Documentation: https://medium.com/@andrewhharmon/apache-airflow-using-pycharm-and-docker-for-remote-debugging-b2d1edf83d9d
	Successfully:
		- packages installed
	Problems:
		Solved: Fail on Linux-like absolute path check
			Workaround: locate to .py file of exception. Comment validation
		Not solved: pwd module not supported in windows
	Conclusion:
		Can't be used

(3) Airflow via WSL. Airflow installed fully inside WSL
	Documentation: https://kontext.tech/article/929/install-airflow-on-windows-via-windows-subsystem-for-linux
	Successfully:
		Installed Python + Airflow in WSL
		Opened and logged in Web UI in browser
		SQLite DB found and can be connected via PyCharm
	Problems:
		SQLite DB tables can't be fetched by PyCharm
			Error encountered when performing Introspect schema main: [SQLITE_BUSY] The database file is locked (database is locked).
				Not supported yet
					https://youtrack.jetbrains.com/issue/DBE-11014
						Exists workaround move database out of wsl
					https://stackoverflow.com/questions/61764101/pycharm-fails-to-connect-to-sqlite-sqlite-busy-database-is-locked
		need found how mount windows my project dags folder to airflow in WSL 
	Conclusion:
		OK. Can be used with limitations.
	Not used finally

(4) Airflow via WSL installation. WSL env but airflow installed to mounted va /mnt/c... folder targeted to my windows project folder
	Documentation: ideas from https://medium.com/international-school-of-ai-data-science/setting-up-apache-airflow-in-windows-using-wsl-dbb910c04ae0
					Use course bash scripts
	Successfully:
		Installed Python in WSL - used previous
		Course .sh script executed
			Problems:
				install_providers.sh
					apache-airflow-providers-apache-hdfs - long execution because of "backtracking".
					This finished during few hours. Last message was something about 2000000 unconfigurable resolution rounds
					I guess not all providers was installed as result
					https://github.com/pypa/pip/issues/11480
	Conclusion:
		OK. Can be used almost in the way as the teacher showed.
		Problems has no impact in my case
	Used finally

