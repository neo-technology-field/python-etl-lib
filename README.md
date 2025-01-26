# Python ETL Toolbox

A library of building blocks to assemble etl pipelines.

So, instead of providing yet another etl tool, the aim is to provide quality building blocks for the usual etl task. These building blocks should (do) meet the following functional requirements:

* logging (of tasks performed including times, errors, and statistics)
* error handling 
* validation of data (currently via Pydantic)
* batching and streaming
* optionally record the information about performed tasks and provide means (NeoDash, console) to review past etl runs.

While this library currently focuses on Neo4j databases, it can be extended to other sources and sinks as needed. 

It does not provide a CLI out of the box, but contains a set of functions to list and manage past runs (if they are stored in a database). In addition, the provided example illustrates how to assemble a etl pipeline and run it from a CLI.

## Quick guide

### Installation

Package is available on PyPi and can be installed via 

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install pip-tools
pip-compile --extra test pyproject.toml
pip-sync
```

### Usage

The below shows a minimalistic etl pipeline to a single CSV file (look at the GTFS example to see more details)

```python

class LoadAgenciesTask(CSVLoad2Neo4jTasks):
    
    class Agency(BaseModel):
        """ Define the Pydantic model for data validation. """
        id: str = Field(alias="agency_id", default="generic")
        name: str = Field(alias="agency_name")
        url: str = Field(alias="agency_url")
        timezone: str = Field(alias="agency_timezone")
        lang: str = Field(alias="agency_lang")

    def __init__(self, context: ETLContext, file:Path):
        super().__init__(context, LoadAgenciesTask.Agency, file)

    def task_name(self) -> str:
        return f"{self.__class__.__name__}('{self.file}')"

    def _query(self):
        """Load the data into Neo4j."""
        return """ UNWIND $batch AS row
        MERGE (a:Agency {id: row.id})
            SET a.name= row.name, 
            a.url= row.url, 
            a.timezone= row.timezone, 
            a.lang= row.lang
        """

    @classmethod
    def file_name(cls):
        return "agency.txt"

context = ETLContext(env_vars=dict(os.environ))

schema = SchemaTask(context=context)
init_group = TaskGroup(context=context, tasks=[schema], name="schema-init")

tasks = [
    LoadAgenciesTask(context=context, file=input_directory / LoadAgenciesTask.file_name()),
]
csv_group = TaskGroup(context=context, tasks=tasks, name="csv-loading")

all_group = TaskGroup(context=context, tasks=[init_group, csv_group], name="main")

context.reporter.register_tasks(all_group)

all_group.execute()

```
See the provided [example](examples/gtfs/README.md) for a more realistic pipeline and how the logging and reporting would look like.

With the above, all lines in the input file `agency.txt` that do not fit the Pydantic model, would be sent to an json file, containing the error data and a description of why it could not be loaded.


