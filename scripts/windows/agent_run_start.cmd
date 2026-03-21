@ECHO OFF

CD /D %~dp0

NODE.EXE agent_run.mjs start %*
