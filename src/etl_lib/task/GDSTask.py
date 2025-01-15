from etl_lib.core.Task import Task, TaskReturn

def transform_dict(input_dict):
    """
    Recursively transforms the input dictionary by converting any dictionary or list values to string representations.

    Helpful to transform a gds call return into a storable representation
    param: input_dict (dict): The input dictionary with values that can be of any type.

    Returns:
        dict: A new dictionary with transformed values.
    """
    def transform_value(value):
        if isinstance(value, dict):
            return {k: transform_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return str(value)
        else:
            return value

    return {key: transform_value(value) for key, value in input_dict.items()}


class GDSTask(Task):

    def __init__(self, context, func):
        """
        Function that uses the gds client to perform tasks. See the following example:

        def gds_fun(etl_context):
            with etl_context.neo4j.gds() as gds:
                gds.graph.drop("neo4j-offices", failIfMissing=False)
                g_office, project_result = gds.graph.project("neo4j-offices", "City", "FLY_TO")
                mutate_result = gds.pageRank.mutate(g_office, tolerance=0.5, mutateProperty="rank")
                return TaskReturn(success=True, summery=transform_dict(mutate_result.to_dict()))

        :param context: The ETLContext to use. Provides the gds client to the func via `etl_context.neo4j.gds()`
        :param func: a function that expects a param `etl_context` and returns a `TaskReturn` object.
        """
        super().__init__(context)
        self.func = func

    def run_internal(self, **kwargs) -> TaskReturn:
        return self.func(etl_context= self.context, **kwargs)
