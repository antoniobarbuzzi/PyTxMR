PyTxMapReduce: Python Twisted MapReduce
==================

Files
-----------

jobtracker.py : implementa il jobtracker di MR, crea un job, comunica coi worker e lancia map, shuffle, reduce
tasktracker.py: implementa un Worker, ovvero un nodo che esegue Map/Reduce

RPC
-----------

jobtracker (jobtracker.py) e workers (tasktracker.py, da rinonimare) comunicano utilizzando Twisted Perspective Broker:
    # Connessione worker to JT
    - un worker si connette al JT (connectToJT)
    - una volta connesso, registra la sua presenza nel JT (registerToJT chiama, tramite Perspective Broker, 'remote_registe'r)
    - il JT gli assegna un worker id (wid)
    
    # JT crea un Job MR
    - Il JT crea un nuovo job MapReduce (stepOneV2)
    - nell'ordine, il JT, tramite Perspective Broker, sempre nella stessa funzione:
        + comunica ai worker la creazione di un nuovo Job, in maniera da permettere l'allocazione di eventuali risorse, chiamado 'remote_initJob'
        + lancia i map sui worker 'remote_executeMap'
        + una volta finiti tutti i worker, inizia lo shuffling [basta mettere i deferred su ciascun callRemote('executeMap') prima di metterlo nella DeferredList per poter avviare lo shuffling dopo la fine di ciascuna map]
        + finito lo shuffling, avvia i reducer
        + finiti i reduce, comunica ai worker che il job può essere distrutto (destroyJob)

Output dei mapper
-----------

L'output dei mapper:
    - salvato su KeyValueStore (in newpartitionspiller.py)
    - esiste un'unica istanza di KeyValueStore x tutti i job
    - il contenuto di KeyValueStore è servito da twisted (vedi newpartitionspiller.py), implementando un IPushProducer
    - in particolare il server serializza l'output (vedi resumeProducing) tramite pickle per ogni KeyValue. Un meccanismo di serializzazione + efficiente sarebbe carino.
    - i reducer si collegano a ciascun worker che ha un keyvaluestore una volta finito ogni map e copiano di dati necessari
    - KeyValueStore serve la lista dei key/value già ordinata; in particolare esso mantiene una lista per ogni tupla (jobid, mapid, partition_number), in maniera tale da permettere la copia dell'output di ciascuna map non appena finita (prova un telnet localhost 8993, scrivendo Job001@Map001@0)
    
    - l'attuale versione di newpartitionspiller ha il limite che non effettua lo spilling dei dati su disco. La prima versione di partition spiller implementata effettuava uno spilling su disco (vedi partitionspiller.py), ma è necessario considerare bene il formato di serializzazione dei dati su disco, su rete, in maniera tale che sia possibile iterare sui dati serializzati e inviarli in rete senza deserializzarli.
    - nota che l'ordinamento avviene prima della serializzazione su disco: nel caso di newpartitionspiller, avviene prima di leggere i dati (getPut)


Programmi MapReduce (WordCount)
-----------

    MapReduceExample.py contiene un programma di esempio che effettua il classico word counting.
    In pratica è necessario passare a python il nome del file contente mapreduce, il nome delle funzioni che effettuano il map e la reduce (in maniera equivalente bisognerà fare x il Reader, ma il discorso è + complicato), e tramite import il tasktracker carica le funzioni da eseguire.
    Lo stile di scrittura dei programmi MR prevede l'uso di yield per ritornare i vari key/value
    In pratica, execute_map in tasktracker.py chiama la funziona importata per ogni key/value di input (leggendo i keyvalue tramite un reader, anch'esso da importare ma ancora nn implementato) e salva i key/value in un'istanza di KeyValueStore (calcolando la partizione di destinazione)
    Ciascuna map è eseguita in un thread usando un DefferredThread (bisogna passare a DeferredProcess, x Big Lock in python)
    
    
Howto
-----------

    - Installa twisted
    - lancia il jobtracker (python jobtracker.py)
    - lancia qualche worker (python tasktracker.py)
    il jobtracker eseguira il count delle parole in warandpeace.txt, solo map, e orchestra shuffling e reducer.

TODO
-----------

Un casino di cose:
    - i reducere devono copiare l'output di ciascun mapper e memorizzarlo in locale (leggendo da KeyValueServer).
    - scrivere la fase di reduce sul modello del mapper (c'è una vecchia versione da usare come canovaccio)
    - i mapper devono leggere i vari chunk, da qualche parte. Da HDFS???? Da un webserver????:
        - python può leggere da hdfs con thrift, ma thrift usa i proxy, quindi nn ha senso
        - python può leggere da hdfs usando python-hdfs (http://github.com/traviscrawford/python-hdfs), ma libhdfs non ha la funzione getBlockList, per conservare la località
    - E' necessario un JobManager, che dato MapReduceExample.py, il file da leggere su hdfs, capisca quanti mapper lanciare (ovvero quanti chunk, quindi bisogna prima decidere da dove i mapper debbano leggere i propri dati)
    - Limitare il numero di processi concorrenti su worker (process pool + rdq)
    

    
