# nsrl_updater

Um utilitário para atualizar e processar dados da NSRL (National Software Reference Library).

## Sobre o Projeto

Este projeto é uma ferramenta desenvolvida em Rust para [ADICIONE AQUI UMA DESCRIÇÃO DETALHADA DO PROPÓSITO DO PROJETO]. Ele visa facilitar o processo de atualização e gerenciamento dos hashes da NSRL, possivelmente interagindo com um banco de dados SQLite para armazenar e consultar essas informações de forma eficiente.

## Funcionalidades

*   **Processamento de dados da NSRL:** Capacidade de ler e processar os arquivos de dados da NSRL.
*   **Interação com Banco de Dados:** Utiliza SQLite para armazenamento e consulta dos dados (inferido pela dependência `rusqlite`).
*   **Processamento Paralelo:** Otimizado para performance utilizando processamento paralelo (inferido pelas dependências `rayon`, `crossbeam`).
*   **Interface de Linha de Comando (CLI):** Interação através de comandos no terminal, com feedback de progresso (inferido pela dependência `indicatif`).
*   **Mapeamento de Memória:** Potencialmente otimizado para lidar com arquivos grandes através de mapeamento de memória (inferido pela dependência `memmap2`).

## Como Começar

Siga estas instruções para obter uma cópia local do projeto em funcionamento.

### Pré-requisitos

*   Rust: Certifique-se de ter o Rust e o Cargo instalados. Você pode instalá-los seguindo as instruções em [rustup.rs](https://rustup.rs/).

### Instalação e Execução

1.  Clone o repositório:
    ```sh
    git clone https://github.com/pmatheus/nsrl_updater.git
    cd nsrl_updater
    ```
2.  Compile o projeto:
    ```sh
    cargo build
    ```
3.  Para uma build otimizada para produção:
    ```sh
    cargo build --release
    ```
4.  Execute o programa:
    ```sh
    cargo run -- [ARGUMENTOS_DO_PROGRAMA_AQUI]
    ```
    Ou, se compilado em modo release:
    ```sh
    ./target/release/nsrl_updater [ARGUMENTOS_DO_PROGRAMA_AQUI]
    ```

## Como Usar

Para executar o `nsrl_updater`, você precisará fornecer o caminho para o arquivo de banco de dados SQLite e pelo menos um arquivo SQL para importação. Argumentos opcionais podem ser usados para ajustar o desempenho.

**Sintaxe básica:**

```sh
# Para Windows (no diretório do projeto, após compilar com 'cargo build')
.\target\debug\nsrl_updater.exe <caminho_para_seu_banco.db> <caminho_para_arquivo.sql> [opções]

# Ou, se compilado em modo release:
.\target\release\nsrl_updater.exe <caminho_para_seu_banco.db> <caminho_para_arquivo.sql> [opções]

# Para Linux/macOS (no diretório do projeto, após compilar com 'cargo build')
./target/debug/nsrl_updater <caminho_para_seu_banco.db> <caminho_para_arquivo.sql> [opções]

# Ou, se compilado em modo release:
./target/release/nsrl_updater <caminho_para_seu_banco.db> <caminho_para_arquivo.sql> [opções]
```

**Argumentos Obrigatórios:**

*   `<caminho_para_seu_banco.db>`: O caminho completo para o arquivo de banco de dados SQLite que será usado ou criado.
*   `<caminho_para_arquivo.sql>`: O caminho completo para o arquivo SQL contendo as instruções a serem importadas. Você pode especificar múltiplos arquivos SQL separando-os por espaços.

**Exemplo com um arquivo SQL:**

```sh
.\target\release\nsrl_updater.exe C:\data\nsrl.db C:\data\NSRLFile.txt
```

**Exemplo com múltiplos arquivos SQL:**

```sh
.\target\release\nsrl_updater.exe C:\data\nsrl.db C:\data\NSRLFile_part1.txt C:\data\NSRLFile_part2.txt
```

**Opções (Argumentos Opcionais):**

*   `--batch-size=N`
    *   Define o número de instruções SQL por transação (lote).
    *   *Padrão*: `25000`
    *   Exemplo: `--batch-size=50000`

*   `--buffer-size=N`
    *   Define o tamanho do buffer de leitura para arquivos SQL, em Megabytes (MB).
    *   *Padrão*: `32` MB
    *   Exemplo: `--buffer-size=64`

*   `--threads=N`
    *   Define o número de threads para processamento paralelo.
    *   *Padrão*: Número de CPUs do sistema.
    *   Exemplo: `--threads=4`

*   `--queue-capacity=N`
    *   Define a capacidade máxima da fila de instruções SQL aguardando processamento.
    *   *Padrão*: `100000`
    *   Exemplo: `--queue-capacity=200000`

*   `--no-mmap`
    *   Desabilita o uso de mapeamento de memória (mmap) para a leitura dos arquivos SQL. Por padrão, o mmap é utilizado se disponível e benéfico.

**Exemplo com opções:**

```sh
.\target\release\nsrl_updater.exe C:\data\nsrl.db C:\data\NSRLFile.txt --threads=8 --batch-size=100000 --buffer-size=128
```

Lembre-se de ajustar os caminhos e opções conforme necessário para o seu ambiente e arquivos.

## Contribuição

Contribuições são o que tornam a comunidade de código aberto um lugar incrível para aprender, inspirar e criar. Quaisquer contribuições que você fizer serão **muito apreciadas**.

Se você tiver uma sugestão para melhorar este projeto, faça um fork do repositório e crie uma pull request. Você também pode simplesmente abrir uma issue com a tag "enhancement".
Não se esqueça de dar uma estrela ao projeto! Obrigado novamente!

1.  Faça um Fork do Projeto
2.  Crie sua Feature Branch (`git checkout -b feature/AmazingFeature`)
3.  Faça Commit de suas Alterações (`git commit -m 'Add some AmazingFeature'`)
4.  Faça Push para a Branch (`git push origin feature/AmazingFeature`)
5.  Abra uma Pull Request

## Licença

Distribuído sob a Licença [NOME_DA_LICENÇA_AQUI]. Veja `LICENSE.txt` para mais informações.

---

Este `README.md` foi gerado com o auxílio de uma IA. Sinta-se à vontade para ajustá-lo conforme necessário!
