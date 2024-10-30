import requests
import os


def baixar_pdf():
    try:
        # Seção 1: Solicitação de informações
        print()
        print("========: Seção 1: Solicitação de informações")
        # reports_server = "https://10.252.134.73/reports/rwservlet?"
        # reports_server = "https://srp-hml.seara.com.br/reports/rwservlet?"
        reports_server = "https://srp.seara.com.br/reports/rwservlet?"

        # servidor = "kc0828star&server=rep_cluster_hml"
        # banco = "kc0828stap"
        banco = "kc0090prdt"
        # report = "&report=/ora01/oracle/hml/exec/"
        report = "&report=/ora01/oracle/prd/exec/"
        desname = "&desname=/ora01/oracle/orarepo/"
        relatorio = "CCME1856"
        # relatorio = "CCME6554"
        # arquivo_pdf = "1125896_00002_00001_N_c0090itaj012-bandeja3-_CCME1859_MIDDLE-EAST-SAUDI-.pdf"
        arquivo_pdf = "teste-jorge.pdf"
        parametros = "&P_CD_LISTA_CARGA=1179631"
        # parametros = "&P_CD_CARGA_EXPORTACAO=650000"

        url_info1 = reports_server
        url_info1 += banco
        url_info1 += report + relatorio
        url_info1 += desname + arquivo_pdf
        url_info1 += parametros

        response_info = requests.get(url_info1, timeout=120, verify=True)
        # response_info = requests.post(url_info1)
        # response_info = requests.get("https://jsonplaceholder.typicode.com/todos/1")
        response_info.raise_for_status()  # Verifica se a requisição foi bem-sucedida
        # dados = response_info.json()  # Supondo que a resposta é um JSON
        dados = response_info.content

        # Converter para bytes-like
        dadosConv = dados.decode("iso-8859-1")
        # Sétima (7) tabela do HTML
        tabelaHtml = dadosConv.split("<table")[7]
        # segunda (2) celula do HTML
        celulaHtml = tabelaHtml.split("<td>")[1]
        # Conteúdo de retorno do reports
        conteudoRetorno = celulaHtml[: celulaHtml.find("</td>") - 1]
        conteudoRetorno = conteudoRetorno.replace("\n", "")
        conteudoRetorno = conteudoRetorno.strip()
        if conteudoRetorno != "The report is successfully run.":
            print("=====: Erro: ", conteudoRetorno)
        else:
            print("=====: Sucesso")
        # Processa os dados recebidos se necessário
        # print("Informações recebidas com sucesso:", dados)

    except requests.RequestException as e:
        print(f"Erro na solicitação de informações: {e}")
        return

    try:
        # Seção 2: Baixar e salvar o arquivo PDF
        print()
        print("========: Seção 2: Baixar e salvar o arquivo PDF")
        # url_pdf = "https://srp-hml.seara.com.br/repout/" + arquivo_pdf
        url_pdf = "https://srp.seara.com.br/repout/" + arquivo_pdf
        # url_pdf = "https://www.caceres.mt.gov.br/fotos_institucional_downloads/2.pdf"

        # response_pdf = requests.get(url_pdf, params=params_pdf, stream=True)
        response_pdf = requests.get(
            url_pdf,
            stream=True,
            # ,
            # proxies={
            #     "https": "http://usertrac:Fusca.Vermelho.5381@mtzsvmfcpprd02:8080"
            # },
        )
        response_pdf.raise_for_status()  # Verifica se a requisição foi bem-sucedida

        # Cria a pasta de destino se não existir
        destino = "./pdfs"
        os.makedirs(destino, exist_ok=True)

        # Caminho completo do arquivo para salvar
        caminho_arquivo = os.path.join(destino, arquivo_pdf)

        # Salva o PDF no caminho especificado
        with open(caminho_arquivo, "wb") as file:
            for chunk in response_pdf.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"=====: Arquivo PDF salvo com sucesso em: {caminho_arquivo}")

    except requests.RequestException as e:
        print(f"Erro ao baixar o arquivo PDF: {e}")
        return
    except IOError as e:
        print(f"Erro ao salvar o arquivo PDF: {e}")
        return


# Exemplo de uso
baixar_pdf()
