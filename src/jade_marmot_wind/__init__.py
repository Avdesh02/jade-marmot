"""Wind turbine telemetry: shared library for the SDP pipeline, the test-data simulator and
the job entry points.

Modules that need Spark import it themselves, so the pure-Python parts (naming, sites,
simulator, validation) stay importable in a plain Python environment.
"""
