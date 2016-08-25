# CES-27 - LAB 1
## Gerenciando Alterações

No passo [Setup](SETUP.md), nós configuramos e fizemos um clone do repositorio do github na máquina local.  

Existem diversos tutoriais sobre a utilização de Gitpela internet, abaixo estão listados alguns:  

[Try Git](https://try.github.io)  
[Git - The Simple Guide](http://rogerdudler.github.io/git-guide/)  
[Git Official Documentation - Getting Started](https://git-scm.com/book/en/v2/Getting-Started-About-Version-Control)  

É interessante que quem nunca teve contato com Git realize pelo menos um dos tutoriais citado para um melhor entendimento dos passos a seguir.

## Primeiro Commit - Passo-a-Passo

Abrir o arquivo *wordcount.go* que se encontra dentro da pasta wordcount no diretório do projeto.  

Na função *mapFunc*, adicionar após
```go
    /////////////////////////
    // YOUR CODE GOES HERE //
    /////////////////////////
```
a seguinte linha
```go
    fmt.Printf("Input Length: %v\n", len(input))
```

Nos imports do arquivo, adicionar o pacote "fmt":
```go
import (
    "fmt"
    "github.com/pauloaguiar/ces27-lab1/mapreduce"
    "hash/fnv"
)
```

Para verificar as nossas alterações:
```shell
wordcount$ go test -v -run Map
```
> === RUN   TestMapFunc  
> Input Length: 0  
> Input Length: 3  
> Input Length: 7  
> Input Length: 13  
> Input Length: 7  
> Input Length: 7  
> Input Length: 8  
> Input Length: 37  
> Input Length: 7  
> --- FAIL: TestMapFunc (0.00s)  
>         wordcount_test.go:123: Description: empty  
>         wordcount_test.go:123: Description: one word  
>         wordcount_test.go:156: Expected: foo : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: two words  
>         wordcount_test.go:156: Expected: foo : 2  ->  Not Found!  
>         wordcount_test.go:123: Description: repeated word  
>         wordcount_test.go:156: Expected: foo : 2  ->  Not Found!  
>         wordcount_test.go:156: Expected: refoo : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: invalid character  
>         wordcount_test.go:156: Expected: foo : 1  ->  Not Found!  
>         wordcount_test.go:156: Expected: bar : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: newline character  
>         wordcount_test.go:156: Expected: bar : 1  ->  Not Found!  
>         wordcount_test.go:156: Expected: foo : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: multiple whitespaces  
>         wordcount_test.go:156: Expected: foo : 1  ->  Not Found!  
>         wordcount_test.go:156: Expected: bar : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: special characters  
>         wordcount_test.go:156: Expected: foo : 7  ->  Not Found!  
>         wordcount_test.go:156: Expected: s : 1  ->  Not Found!  
>         wordcount_test.go:123: Description: uppercase characters  
>         wordcount_test.go:156: Expected: foo : 2  ->  Not Found!  
> FAIL  
> exit status 1  
> FAIL    github.com/pauloaguiar/ces27-lab1/wordcount     0.035s  

No inicio podemos visualizar a saída do comando que foi introduzido no código. Note que os testes falham pois ainda não introduzimos o código que realiza corretamente a operação de Map.

Para ver o estado das nossas alterações, executamos o seguinte comando:

```shell
wordcount$ git status
```
> On branch master  
> Your branch is up-to-date with 'origin/master'.  
> Changes not staged for commit:  
>   (use "git add <file>..." to update what will be committed)  
>   (use "git checkout -- <file>..." to discard changes in working directory)> 
>
>         modified:   wordcount.go>   
>
> no changes added to commit (use "git add" and/or "git commit -a")  

Precisamos selecionar os arquivos para a etapa de Commit.

```shell
$ git add wordcount/wordcount.go
```
>  

```shell
$ git status
```
> On branch master  
> Your branch is up-to-date with 'origin/master'.  
> Changes to be committed:  
>   (use "git reset HEAD <file>..." to unstage)>   
>   
>         modified:   wordcount.go  

Agora que as nossas alterações foram selecionadas, vamos realizar o commit:

```shell
$ git commit
```

O console vai abrir o VIM(Calma, vamos sobreviver...) caso nenhum outro editor tenha sido configurado. Escrever um comentário que descreva as alterações feitas:
```
Imprimindo tamanho do input
# Please enter the commit message for your changes. Lines starting
# with '#' will be ignored, and an empty message aborts the commit.
# On branch master
# Your branch is up-to-date with 'origin/master'.
#
# Changes to be committed:
#       modified:   wordcount.go
#
# Changes not staged for commit:
#       modified:   ../ENTREGA.md
#
# Untracked files:
#       ../GIT.html
#       ../GIT.md
#       ../README.html
#       ../SETUP.html
#       ../SETUP.md
```

Para gravar o arquivo, você deve apertar **ESC** para sair do modo de inserção(-- INSERT --), apertar **SHIFT+;** para abrir a linha de comando do VIM, digitar **wq** e apertar **ENTER**.

> [master a68a570] Imprimindo tamanho do input  
> 1 file changed, 5 insertions(+), 1 deletion(-)

Agora as nossas alterações foram adicionadas ao nosso repositório local:

```shell
$ git log -n 1
```
> commit a68a570562a41bd26b485dcd80ec2592b8e4c4a9  
> Author: Paulo Araujo <phaguiardm@gmail.com>  
> Date:   Wed Aug 24 21:04:56 2016 -0300>   
>  
>     Imprimindo tamanho do input  


Para enviar as suas alterações para o seu repositório remoto (Fork no GitHub), seguir instruções em: [Entrega](ENTREGA.md)