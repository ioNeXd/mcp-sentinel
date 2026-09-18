"""Scripts utilitários do Gateway (pacote para resolução única de módulos).

O ``gateway/http_server.py`` importa ``scripts.import_claude_desktop_config``
e a suíte de testes carrega o MESMO arquivo como módulo top-level standalone
(``importlib.util.spec_from_file_location``). Sem ``__init__.py``, o mypy
descobria o arquivo sob os DOIS nomes e recusava checar
``scripts/``/``main.py`` ("Source file found twice under different module
names"). Como pacote, o nome canônico é ``scripts.import_claude_desktop_config``
e o escopo de tipo cobre o script — o mesmo arquivo que as fases de revisão
(achados 1/10/25) mostraram ser superfície de regressão real.
"""
