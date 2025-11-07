raise NotImplementedError(
    """
result = subprocess.run(
            ["ssh", self.host, remote_command],
            text=True,
            capture_output=True,
        )

where remote_command is: cd <bufferdir> && mkdir -p <projectsubdir>
(if mode is rw rather than ro)
"""
)
