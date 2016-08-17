# ENTREGA DOS TRABALHOS
Para as práticas do curso de CES27, utilizaremos o Github para entrega dos trabalhos.

Com o objetivo de aproximar os alunos das práticas comuns na indústria de software, vamos utilizar algumas *features* que a plataforma fornece:
 * Fork
 * Pull Request
 * Code Review 

Além disso, o aluno também deve ser familiar com as práticas comuns no controle de versão utilizando Git:
 * Commit
 * Pull/Push
 * Merge
 * Remote Repositories
 * Branching

Dado que o foco do nosso curso é o aprendizado e a confiança que é esperada na relação aluno/professor, vamos estabelecer um código de honra para as atividades.

### Código de Honra
 * Não terei contato com os trabalhos realizados por outros alunos enquanto não tiver entregado o meu trabalho, incluindo trechos de código apresentados nas Pull Requests ou nos repositórios pessoais.
 * Todo o código utilizado que não for escrito por mim, terá seu autor ou referência citada.
 * Não discutirei detalhes (estruturas, algoritmos e trechos de código) das soluções dos problemas propostos com outros alunos.
 
Dado isso, fica livre aos alunos compartilharem **casos de teste** e suas respectivas saídas.

## Preparando o repositório no GitHub

Todos os alunos devem possuir uma conta no [GitHub](https://github.com).

Acessar o seguinte repositório: [CES-27 - Lab 1](https://github.com/PauloAguiar/ces27-lab1)

Fazer um Fork do repositório clicando no seguinte botão: ![Fork Button](https://help.github.com/assets/images/help/repository/fork_button.jpg)

Agora associado à sua conta existe uma cópia do repositório do laboratório.

![Forked Repository](doc/forked-repo.png?raw=true)
## Preparando o Repositório local

Os comandos a seguir requerem uma instalação da ferramenta [Git](https://git-scm.com/). Para verificar a sua instalação basta digitar o seguinte comando no prompt:

**$ git --version**
>git version 2.9.2.windows.1

Além disso, as ferramentas de Go devem estar devidamente instaladas e configuadas, incluindo o GOPATH (ver [Go - Getting Started](https://golang.org/doc/install) em caso de dúvidas). Para verificar a sua instalação, basta digitar no prompt o seguinte comando:

**$ go env**
>set GOARCH=amd64
>set GOBIN=
>set GOEXE=.exe
>set GOHOSTARCH=amd64
>set GOHOSTOS=windows
>set GOOS=windows
>set GOPATH=C:\gows
>set GORACE=
>set GOROOT=C:\tools\Go
>set GOTOOLDIR=C:\tools\Go\pkg\tool\windows_amd64
>set GO15VENDOREXPERIMENT=1
>set CC=gcc
>set GOGCCFLAGS=-m64 -mthreads -fmessage-length=0
>set CXX=g++
>set CGO_ENABLED=1

Com todas as ferramentas devidamente configuradas, obtenha os arquivos do lab usando o seguinte comando:

**$ go get -d github.com/pauloaguiar/ces27-lab1**
> package github.com/PauloAguiar/ces27-lab1: no buildable Go source files in /home/brugger/work/src/github.com/PauloAguiar/ces27-lab1

Esse comando vai colocar no seu *workspace* os arquivos do laboratório, no diretório *src/github.com/pauloaguiar/ces27-lab1*.

Você não deve alterar o diretório, pois isso quebrará as referências internas do código. Ao invés disso, vamos adicionar o seu fork como um repositório remoto. Altere o a parte com CHANGE_USER para o seu usuario do github e execute o comando abaixo:

**$ git remote add fork https://github.com/CHANGE_USER/ces27-lab1 **

**$ git remote -v**
>fork   https://github.com/USER/ces27-lab1/ces27-lab1 (fetch)
>fork   https://github.com/USER/ces27-lab1/ces27-lab1 (push)
>origin https://github.com/pauloaguiar/ces27-lab1/ces27-lab1 (fetch)
>origin https://github.com/pauloaguiar/ces27-lab1/ces27-lab1 (push)

Com isso você pode agora realizar as alterações pedidas para gerar a solução do laboratório no novo diretório.

### Code Review

Os alunos podem, a qualquer momento dentro do prazo estabelecido para code review, enviar uma pull request (ver procedimento de pull request abaixo) onde o aluno pode pedir uma revisão do seu código ou tirar alguma dúvida (podendo ser até mesmo uma solução parcial).

## Enviando a Solução

Quando estiver pronto para enviar a sua solução, o aluno deve dar *push* do seu repositório local para o repositório remoto *fork*:

**$ git push fork**

Utilizar as suas credênciais do github.

Com a sua solução já no seu repositório remoto, você deve ir até a página do mesmo e criar uma *pull request*:

![New Pull Request](https://help.github.com/assets/images/help/pull_requests/pull-request-start-review-button.png)

Ná página seguinte você pode verificar as alterações que estão sendo enviadas.

Quando concluir, clicar no botão motrado abaixo:

![Create Pull Request](https://help.github.com/assets/images/help/pull_requests/pull-request-click-to-create.png)

Preencher os dados da *pull request*.

 * Se for a solução final, especificar com o título *Solução do Lab* ou similar.
 * Caso seja uma dúvida, pedido de code review ou outro, especifica no título o pedido e colocar na descrição os detalhes, podendo também adicionar comentários diretamente no código.

![Pull Request](https://help.github.com/assets/images/help/pull_requests/pullrequest-description.png)

A primeira *pull request* deve ser feita no *branch* **master** do repositório base. Após isso será criada um branch com nome igual ao seu usuário do github no repositório base. Todos os *pull requests* seguintes devem ser feitos diretamente nesse novo *branch*.

![Branches](doc/branches.png?raw=true)

Você pode enviar mais de um *pull request* com a sua solução, somente o estado final do seu *branch* até a data limite será considerado.

## Colaborando

Todos os alunos estão convidados a colaborar com o conteúdo da prática, propondo mudanças, relatando erros ou enviando *pull requests* com melhorias.

Para relatar erros, por favor utilizar a ferramenta Issues do github no seguinte link:

[Issues](https://github.com/PauloAguiar/ces27-lab1/issues)

Para enviar propostas de alterações, basta utilizar a ferramenta de *pull request* como mostrada acima, descrevendo nos comentários a mudança proposta. Nesse caso você deve fazer a request para o branch master do repositório base.