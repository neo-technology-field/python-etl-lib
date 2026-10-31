import abc
from pathlib import Path

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


class SchemaTask(ExecuteCypherTask, abc.ABC):
    """Run explicitly requested schema setup through the normal task lifecycle.

    Subclasses supply their statements from ``_schema_statements()``. Construction has no
    effect on the database, and no schema change is applied at application startup.
    Statements run only when the task is executed. A task built by :py:meth:`from_file`
    reads its file at that point, so a missing file or an unterminated quote fails the
    execution instead of construction.

    Idempotency belongs to the caller. Supply statements that tolerate being run again,
    normally with ``IF NOT EXISTS``, or a second execution will fail.
    """

    def _query(self) -> list[str]:
        """Return the statements to execute.

        Returns:
            The statements returned by ``_schema_statements()``, in execution order.
        """
        return self._schema_statements()

    @abc.abstractmethod
    def _schema_statements(self) -> list[str]:
        """Return schema statements in execution order.

        Each entry is one Cypher statement. Omitting the trailing semicolon is a
        convention, not a requirement: statements reach the executor unchanged, so a
        terminator supplied by a subclass is sent as written. Statements run one after
        another, each in its own managed transaction, so a failure part way through
        leaves the earlier statements applied.

        Returns:
            The Cypher statements to execute.
        """
        raise NotImplementedError

    @classmethod
    def from_file(cls, context: ETLContext, path: Path) -> "SchemaTask":
        """Create a task that reads the schema file when executed.

        The file is not read during construction. It is read when the task runs, so a
        missing or malformed file fails that execution rather than the setup of the
        pipeline.

        Args:
            context: The ETL context providing the Neo4j connection.
            path: Path to a UTF-8 file holding semicolon separated Cypher statements. The
                final statement may omit its terminator.

        Returns:
            A schema task bound to ``path``.
        """
        return _FileSchemaTask(context, path)


class _FileSchemaTask(SchemaTask):
    """File-backed schema task, created through ``SchemaTask.from_file()``."""

    def __init__(self, context: ETLContext, path: Path) -> None:
        super().__init__(context)
        self._path = path

    def _schema_statements(self) -> list[str]:
        return _split_statements(self._path.read_text(encoding="utf-8"))


def _split_statements(text: str) -> list[str]:
    """Split schema statements without treating quoted/commented semicolons as delimiters.

    This is lexical statement splitting, not a Cypher parser and not a syntax validator.
    Single quotes, double quotes, backtick identifiers, escaped quotes, doubled quote
    characters and ``//`` or ``/* */`` comments are respected. Shell commands such as
    ``:use``, include directives and migration directives are not supported.

    Args:
        text: The contents of a schema file.

    Returns:
        The non-empty statements in file order, with comments removed and without their
        trailing semicolons.

    Raises:
        ValueError: If a quote or block comment is never closed.
    """
    statements: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    comment: str | None = None
    position = 0

    while position < len(text):
        character = text[position]
        pair = text[position:position + 2]

        if comment == "//":
            if character == "\n":
                comment = None
                buffer.append("\n")
        elif comment == "/*":
            if pair == "*/":
                comment = None
                buffer.append(" ")
                position += 1
        elif quote is not None:
            buffer.append(character)
            if character == "\\" and quote != "`" and position + 1 < len(text):
                position += 1
                buffer.append(text[position])
            elif character == quote:
                if position + 1 < len(text) and text[position + 1] == quote:
                    position += 1
                    buffer.append(text[position])
                else:
                    quote = None
        elif pair in ("//", "/*"):
            comment = pair
            buffer.append(" ")
            position += 1
        elif character in ("'", '"', "`"):
            quote = character
            buffer.append(character)
        elif character == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer.clear()
        else:
            buffer.append(character)

        position += 1

    if quote is not None or comment == "/*":
        raise ValueError("Unterminated quote or block comment in schema file")
    statement = "".join(buffer).strip()
    if statement:
        statements.append(statement)
    return statements
