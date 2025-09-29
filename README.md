# Trade Recap

Tool for automatizing the daily trading report.

The project fetch information via a API, and then it manage data to separate New and Early termination trade from others types

*Note* : This is an optimised and updated version of an existing project with the same name. 

## Set up

You need to install dependecies from [`requirements.txt`](./requirements.txt) in order to run the project. There are two ways to do it

### Virtual Environement

Open a `terminal` or a `Windows PowerShell` in the root path project.

in the project directory
```bash
cd trade-reacp
```

Let's create the virtual environement

```bash
python -m venv .
```
> You can use `python3` but it depends on you system

Depending on you Operating System (`Windows` or `Linux`), the directories names and architecture created can differ.

#### Windows

The previous command should create the following folders
```
Include
Lib
Scripts
```
> Inside `Scripts` directory, you will find the script we need to active the virtual environement


Given that we are using a `Windows Powershell`, use the `ps1` activator
```bash
.\Scripts\Activate.ps1
```

If it's all good, a text should appear before the console log like this
```
(trade-recap) PS \your\path\to\trade-reacp > 
```

Now we are ready to install dependencies


#### Linux

The following directories will be created 

```bash
bin/
lib/
include/
pyvenv.cfg
```
> the `pyvenv.cfg` file a configuration file, you can ignore it

This time, the activator scripts are in the `bin` folder. Depending on your `shell`, pick up the best for you (`fish`, `bash`, etc)

Let's use the common one: `bash`
```bash
source bin/activate.bash
```

Same as windows, your prompt should change and show the trade-recap environment
```bash
(trade-recap) username@host /path/to/trade-recap $ 
```
> This is an abstraction , it can differ from your terminal config

All is now good for installing dependecies


### Native

This method avoid passing by a virtual environment method and install depedencies directly in your system

At this instance, you don't need to set up anything in particular, just install the dependecies as you will see in the next section

***Disclaimer*** : As well as dependecies will be installed directly in your system, this can create version and packages conflicts. In particular, it can corrupt different package files.

**Note** : Use this method only if you install python package via your system package manager eg. `pacman`, `apt`, etc. (and not `pip3`)

## Installing Dependencies

Once the virtual environement correctly set up, it's time to install project's dependecies

Run the next line into your `terminal` or `Windows PowerShell`
```bash
pip3 install -r requirements.txt
```

You will see your output installing packages. Once it's donne, you will have a successfull message
```
Installing collected packages: urllib3, idna, charset_normalizer, certifi, polars
...
Installing collected packages: urllib3, idna, charset_normalizer, certifi, requests
``` 

## Run the project

In the same ``terminal`` or `Windows PowerShell`, just run the [`main.py`](./main.py) file

```
python main.py
```

The previous command will create two different files, both placed in the (new) `data` folder

The first file it's the raw excel file of the trade information (no treating) and the second one, exploded, treated and formatted information for sending report


