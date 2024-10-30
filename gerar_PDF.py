from conexao_oracle import conexao_oracle
from logger import logger
from threading import Thread, Semaphore, current_thread
from datetime import datetime
import subprocess
import os
import requests


class geracao_PDF:
    def __init__(self, ambiente, qtdeThreads, dev=False):
        ############################################
        # PARAMETRIZAÇÃO
        ############################################
        self.ambiente = ambiente
        self.dev = dev

        ############################################
        # HOMOLOGAÇÃO
        ############################################
        if ambiente == "hom":
            self.database = "sta"
            self.motor_reports = "C:\orant\BIN\RWRUN60.EXE"
            self.user_id = "USERID=usuario/senha@banco"
            self.caminho_relatorio = "MODULE=//c0828/obj_pc/usr/procger/CCME/"
            # REPORTS 12C
            self.reports_server = "https://srp-hml.seara.com.br/reports/rwservlet?"
            # if dev:
                # self.reports_server = "https://10.252.134.73/reports/rwservlet?"
            # self.banco = "kc0828stap"
            self.banco = "kc0828stat"
            self.report = "&report=/ora01/oracle/hml/exec/"
            self.desname = "&desname=/ora01/oracle/orarepo/"
            self.reports_server_PDF = "https://srp-hml.seara.com.br/repout/"

        ############################################
        # PRODUÇÃO
        ############################################
        elif ambiente == "prd":
            self.database = "prd"
            self.motor_reports = "C:\orant\BIN\RWRUN60.EXE"
            self.user_id = "USERID=usuario/senha@banco"
            self.caminho_relatorio = "MODULE=//c0090/obj_pc/usr/procger/CCME/"
            # REPORTS 12C
            self.reports_server = "https://srp.seara.com.br/reports/rwservlet?"
            # self.banco = "kc0090prdp"
            self.banco = "kc0090prdt"
            self.report = "&report=/ora01/oracle/prd/exec/"
            self.desname = "&desname=/ora01/oracle/orarepo/"
            self.reports_server_PDF = "https://srp.seara.com.br/repout/"

        ############################################
        # PARAMETROS GERAIS
        ############################################
        self.parametros_fixos = "TRACEOPTS=TRACE_ERR DESFORMAT=PDF DESTYPE=FILE MODE=BITMAP PARAMFORM=NO ORACLE_SHUTDOWN=YES PRINTJOB=NO BATCH=YES"
        # Quantidade de THREADS em paralelo
        self.semaforo = Semaphore(qtdeThreads)

        ############################################
        # CONECTAR ORACLE
        ############################################
        self.oracle = conexao_oracle(self.database, dev)

    def gerar_arquivos_PDF(
        self, result_arq=(), ret_arquivo=[], indice=0, tipo_fila=0, tp_impressao=None
    ):
        # LOOP POR ARQUIVO
        logger.info(
            f"{tp_impressao} - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}"
        )
        try:
            # ds_relatorio = 'MODULE=//c0828/obj_pc/usr/procger/CCME/' + result_arq[3] + '.rep'
            ds_relatorio = self.caminho_relatorio + result_arq[3] + ".rep"
            ds_parametros_relatorio = result_arq[4]
            ds_arquivo_PDF = "DESNAME=" + result_arq[5]
            ds_erro_file = "ERRFILE=" + result_arq[5][:-4] + ".txt"
            ds_trace_file = "TRACEFILE=" + result_arq[5][:-4] + ".log"
            #
            comando_pdf = self.motor_reports + " "
            comando_pdf += ds_relatorio + " "
            comando_pdf += ds_parametros_relatorio + " "
            comando_pdf += ds_arquivo_PDF + " "
            comando_pdf += ds_erro_file + " "
            comando_pdf += ds_trace_file + " "
            # comando_pdf += self.user_id + " "
            comando_pdf += self.parametros_fixos + " "
            #
            executa_novamente = 1
            while executa_novamente > 0:
                result_sub = subprocess.run(
                    comando_pdf + self.user_id
                    # ,capture_output=True
                    # ,text=True
                    ,
                    timeout=300,  # 5 minutos
                    # ,input=b"underwater"
                )
                # print("stdout:", result.stdout)
                # print("stderr:", result.stderr)

                # Analisar execução com sucesso/erro
                # Pegar o retorno da execução 0-Ok, X-Erros
                # Se diferente de zero então é erro
                # TENTAR NOVAMENTE QUANDO RETURNCODE 3221225477
                if result_sub.returncode == 3221225477 and executa_novamente == 1:
                    executa_novamente = 2
                    logger.info(
                        f"{tp_impressao} - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]} - NOVA TENTATIVA"
                    )
                else:
                    executa_novamente = 0

            ret_arquivo[indice] = result_sub.returncode

            # Pegar conteúdo do arquivo se returncode diferente de zero
            erro = None
            # ret_arquivo[indice] = 1 - teste de erro

            # Analisar conteúdo do arquivo trace
            try:
                arquivo = open(ds_trace_file[10:], "r")
                arquivo_inteiro = None
                arquivo_inteiro = arquivo.read()
                arquivo.close()
                os.remove(ds_trace_file[10:])

                # verificar se existe erro
                if "MSG" in arquivo_inteiro or "ERR" in arquivo_inteiro:
                    erro = f"{tp_impressao} - TRACEFILE - Processo: {result_arq[2]}, relatório: {result_arq[3]}, ReturnCode: {result_sub.returncode}\narquivo: {result_arq[5][:-4] + '.pdf'}\ntrace: {result_arq[5][:-4] + '.log'}\n\nConteúdo trace:\n{arquivo_inteiro}"
                    ret_arquivo[indice] = 27

            except Exception as Erro:
                erro = str(Erro)
                erro = f"{tp_impressao} - ERRO LER TRACE - Processo: {result_arq[2]}, relatório: {result_arq[3]}, ReturnCode: {result_sub.returncode}\n\nComando PDF: {comando_pdf}\n\nConteúdo trace:\n{erro}"
                logger.error(erro)

            # Mudar status fila geração PDF e guardar conteúdo arquivo
            if erro != None:
                self.oracle.prc_processar_fila_pdf(
                    p_cd_sequencia=result_arq[0],
                    p_cd_tipo_fila=tipo_fila,
                    p_id_status=0,
                    p_cd_sequencia_sub=result_arq[1],
                    p_ds_log=erro,
                )
                logger.error(erro)
                self.oracle.send_email(
                    "CCME",
                    "Gerar PDF - Impressão Automática <ERRO CCME1901>",
                    erro,
                )
        except Exception as Erro:
            msg = f"{tp_impressao} - Erro geral - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}\n\nerro: {str(Erro)}"
            logger.error(msg)
            ret_arquivo[indice] = 99
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=tipo_fila,
                p_id_status=0,
                p_cd_sequencia_sub=result_arq[1],
                p_ds_log=msg,
            )

    def gerar_arquivos_PDF_12C(
        self, result_arq=(), ret_arquivo=[], indice=0, tipo_fila=0, tp_impressao=None
    ):
        # LOOP POR ARQUIVO
        logger.info(
            f"{tp_impressao} - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}"
        )

        # Geral
        try:
            # Controle de erro
            erro = None

            # Nome arquivo destino
            ultima_barra = result_arq[5].rfind("\\")
            arquivo_destino = result_arq[5][ultima_barra + 1 :]

            # Reports server não funciona quando arquivo possui dois hífens consecutivos
            arquivo_destino = arquivo_destino.replace("--", "-")

            # Seção 1: Solicitação de informações
            if True:
                try:
                    # Parâmetros relatorio
                    parametros_relatorio = "&" + result_arq[4]
                    parametros_relatorio = parametros_relatorio.replace(" ", "&")

                    # URL request
                    url_request = self.reports_server
                    url_request += self.banco
                    url_request += self.report + result_arq[3]
                    url_request += self.desname + arquivo_destino
                    url_request += parametros_relatorio

                    # Dev
                    verifySSL = True
                    if self.ambiente == "hom" and self.dev:
                        verifySSL = False

                    # Request na API
                    response_info = requests.get(
                        url_request, timeout=120, verify=verifySSL
                    )
                    # Verificar se a requisição foi bem-sucedida
                    response_info.raise_for_status()
                    # Obter HTML de resposta
                    dados = response_info.content

                    # Converter para bytes-like
                    dadosConv = dados.decode("iso-8859-1")
                    # Sétima (7) tabela do HTML
                    tabelaHtml = dadosConv.split("<table")[7]
                    # Segunda (2) celula do HTML
                    celulaHtml = tabelaHtml.split("<td>")[1]
                    # Conteúdo de RETORNO REPORTS
                    conteudoRetorno = celulaHtml[: celulaHtml.find("</td>") - 1]
                    conteudoRetorno = conteudoRetorno.replace("\n", "")
                    conteudoRetorno = conteudoRetorno.strip()

                    # Analisar RETORNO REPORTS
                    if conteudoRetorno != "The report is successfully run.":
                        erro = conteudoRetorno
                        erro = f"{tp_impressao} - ERRO GERAR PDF - Processo: {result_arq[2]}, relatório: {result_arq[3]}\nErro: {erro}\nURL Request: {url_request}"
                        logger.error(erro)
                        ret_arquivo[indice] = 1

                except Exception as e:
                    erro = str(e)
                    erro = f"{tp_impressao} - ERRO REQUISIÇÃO - Processo: {result_arq[2]}, relatório: {result_arq[3]}\nErro: {erro}\nURL Request: {url_request}"
                    logger.error(erro)
                    ret_arquivo[indice] = 2

            # Seção 2: Baixar e salvar o arquivo PDF
            if erro == None:
                # Baixar arquivo
                try:
                    url_pdf = self.reports_server_PDF + arquivo_destino
                    response_pdf = requests.get(
                        url_pdf,
                        stream=True,
                    )
                    response_pdf.raise_for_status()  # Verifica se a requisição foi bem-sucedida

                except Exception as e:
                    erro = str(e)
                    erro = f"{tp_impressao} - ERRO BAIXAR PDF - Processo: {result_arq[2]}, relatório: {result_arq[3]}\nErro: {erro}\nURL Request: {url_pdf}"
                    logger.error(erro)
                    ret_arquivo[indice] = 5

                # Salvar o PDF no caminho especificado
                if erro == None:
                    try:
                        with open(result_arq[5], "wb") as file:
                            for chunk in response_pdf.iter_content(chunk_size=8192):
                                file.write(chunk)

                    except Exception as e:
                        erro = str(e)
                        erro = f"{tp_impressao} - ERRO SALVAR PDF - Processo: {result_arq[2]}, relatório: {result_arq[3]}\nErro: {erro}\nURL Request: {result_arq[5]}"
                        logger.error(erro)
                        ret_arquivo[indice] = 10

            # Mudar status fila geração PDF e guardar conteúdo arquivo
            if erro != None:
                self.oracle.prc_processar_fila_pdf(
                    p_cd_sequencia=result_arq[0],
                    p_cd_tipo_fila=tipo_fila,
                    p_id_status=0,
                    p_cd_sequencia_sub=result_arq[1],
                    p_ds_log=erro,
                )
                # logger.error(erro)
                self.oracle.send_email(
                    "CCME",
                    "Gerar PDF - Impressão Automática <ERRO CCME1901>",
                    erro,
                )
        except Exception as Erro:
            msg = f"{tp_impressao} - Erro geral - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}\nErro: {str(Erro)}"
            logger.error(msg)
            ret_arquivo[indice] = 99
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=tipo_fila,
                p_id_status=0,
                p_cd_sequencia_sub=result_arq[1],
                p_ds_log=msg,
            )

    def impressao_automatica(self, result_proc=[]):
        result_arquivos = []
        try:
            # raise Exception("erro teste")
            result_arquivos = self.oracle.ler_fila(
                cd_tipo_fila=1, result_proc=result_proc
            )
        except Exception as Erro:
            msg = f"impressao_automatica - ler_fila - processo: {result_proc[1]}, sequencia: {result_proc[0]}, Erro: {str(Erro)}"
            logger.error(msg)

        if len(result_arquivos) != 0:
            logger.info(f"processo: {result_proc[1]}, sequencia: {result_proc[0]}")

            # Vetor de retorno para threads
            ret_arquivos = [0] * 10
            indice = -1
            # Gerar cada arquivo em um Thread diferente
            threads = list()
            for result_arq in result_arquivos:
                indice += 1
                t = Thread(
                    # target=self.gerar_arquivos_PDF,
                    target=self.gerar_arquivos_PDF_12C,
                    args=(result_arq, ret_arquivos, indice, 1, "impressao_automatica"),
                )
                threads.append(t)
                t.start()

            for index, thread in enumerate(threads):
                thread.join()

            # verificar se houve erro no processamento de algum arquivo
            v_id_status = 1
            for i in ret_arquivos:
                if i != 0:
                    v_id_status = 3

            # Mudar status fila geração PDF
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=1,
                p_id_status=v_id_status,
                p_cd_sequencia_sub=None,
                p_ds_log=None,
            )

    def shipment_details(self, result_proc=[]):
        result_arquivos = []
        try:
            # raise Exception("erro teste")
            result_arquivos = self.oracle.ler_fila(
                cd_tipo_fila=2, result_proc=result_proc
            )
        except Exception as Erro:
            msg = f"shipment_details - ler_fila - processo: {result_proc[1]}, sequencia: {result_proc[0]}, Erro: {str(Erro)}"
            logger.error(msg)
            # return

        if len(result_arquivos) != 0:
            logger.info(f"processo: {result_proc[1]}, sequencia: {result_proc[0]}")
            # Gerar cada arquivo em um Thread diferente
            threads = list()
            ret_arquivos = [0] * 10
            indice = -1
            for result_arq in result_arquivos:
                indice += 1
                t = Thread(
                    # target=self.gerar_arquivos_PDF,
                    target=self.gerar_arquivos_PDF_12C,
                    args=(result_arq, ret_arquivos, indice, 2, "shipment_details"),
                )
                threads.append(t)
                t.start()

            for index, thread in enumerate(threads):
                thread.join()

            # verificar se houve erro no processamento de algum arquivo
            v_id_status = 1
            for i in ret_arquivos:
                if i != 0:
                    v_id_status = 3
            # Mudar status fila geração PDF
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=2,
                p_id_status=v_id_status,
                p_cd_sequencia_sub=None,
                p_ds_log=None,
            )

    def executar_processo(self, result_proc=[]):
        # logger.info(f"INICIO: {current_thread().name}")
        with self.semaforo:
            # IMPRESSAO AUTOMATICA
            # threads = list()
            # t = Thread(
            #     target=self.impressao_automatica,
            #     args=(result_proc,),
            # )
            # threads.append(t)
            # t.start()
            self.impressao_automatica(result_proc)

            # SHIPMENT DETAILS
            # tt = Thread(
            #     target=self.shipment_details,
            #     args=(result_proc,),
            # )
            # threads.append(tt)
            # tt.start()
            self.shipment_details(result_proc)

            # AGUARDAR FINALIZAÇÃO DAS THREADS
            # for index, thread in enumerate(threads):
            #     thread.join()

    def executar(self):
        try:
            # Conectar Oracle
            self.oracle.connect()

            # LOOP POR SEQUENCIA/PROCESSO
            result_processos = self.oracle.ler_fila_processos()
            if len(result_processos) == 0:
                logger.info("Não existem processos na Fila")
                self.oracle.disconnect()
                return

            # Gerar cada processo em um Thread diferente
            threads = list()
            for result_proc in result_processos:
                t = Thread(
                    target=self.executar_processo,
                    args=(result_proc,),
                )
                threads.append(t)
                t.name = f"<THREAD {len(threads)}> Sequencia: {result_proc[0]} Processo: {result_proc[1]}"
                logger.info(t.name)
                t.start()

            for index, thread in enumerate(threads):
                thread.join()

            self.oracle.disconnect()
        except Exception as Erro:
            self.oracle.disconnect()
            msg = f"executar Erro: {str(Erro)}"
            logger.error(msg)


if __name__ == "__main__":
    ############################################
    # AMBIENTE EXECUÇÃO
    ############################################
    ambiente = "prd"
    dev = False
    qtdeThreads = 3

    # DEVELOPER
    # ambiente = "hom"
    # dev = True

    ############################################
    # Data e hora - INICIO
    ############################################
    agora = datetime.now()  # current date and time
    data_com_hora = agora.strftime("%d/%m/%Y, %H:%M:%S")
    logger.info(f"======= <INICIO> - Data: {data_com_hora} =======")

    ############################################
    # EXECUTAR PROCESSAMENTO
    ############################################
    gerar_PDF = geracao_PDF(ambiente, qtdeThreads, dev)
    gerar_PDF.executar()

    ############################################
    # Data e hora - FINAL
    ############################################
    agora = datetime.now()  # current date and time
    data_com_hora = agora.strftime("%d/%m/%Y, %H:%M:%S")
    logger.info(f"======= <FIM> - Data: {data_com_hora} =======")
