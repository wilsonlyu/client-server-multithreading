#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "FIFOreqchannel.h"
#include <time.h>
#include <thread>
#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
using namespace std;

HistogramCollection hc;
FIFORequestChannel* create_new_channel (FIFORequestChannel* mainchan){
    char name [1024];
    MESSAGE_TYPE m = NEWCHANNEL_MSG;
    mainchan->cwrite(&m, sizeof(m));
    mainchan->cread(name,1024);
    FIFORequestChannel* newchan = new FIFORequestChannel(name,FIFORequestChannel::CLIENT_SIDE );
    return newchan;

}

void patient_thread_function(int n, int pno, BoundedBuffer* request_buffer){
    datamsg d (pno,0.0,1);
    double resp = 0;
    for (int i=0;i<n;i++){

        request_buffer->push((char *)&d, sizeof(datamsg));
        d.seconds += 0.004;

    }
    
}

void file_thread_function (string fname, BoundedBuffer* request_buffer,FIFORequestChannel* chan, int mb){
    string recvfname = "recv/"+fname;
    
    char buf[1024];
    filemsg f(0,0);
    memcpy(buf,&f,sizeof(f));
    strcpy(buf+sizeof(f), fname.c_str());
    chan->cwrite(buf,sizeof(f)+fname.size()+1);
    __int64_t filelength;
    chan->cread(&filelength,sizeof(filelength));

    FILE* fp = fopen(recvfname.c_str(),"w");
    fseek(fp,filelength,SEEK_SET);
    fclose(fp);

    filemsg* fm = (filemsg*) buf;
    __int64_t remlen = filelength;

    while (remlen>0){
        fm->length = min (remlen, (__int64_t) mb);
        request_buffer->push(buf,sizeof(filemsg)+fname.size()+1);
        fm->offset += fm->length;
        remlen -=fm->length;
    }
}

void worker_thread_function(FIFORequestChannel* chan, BoundedBuffer* request_buffer, HistogramCollection* hc, int mb){
    char buf [1024];
    double resp = 0;
    char recvbuf [mb];
    while(true){
        request_buffer->pop(buf, 1024);
        MESSAGE_TYPE* m = (MESSAGE_TYPE *) buf;
        if(*m==DATA_MSG){
            chan->cwrite(buf, sizeof(datamsg));
            chan->cread(&resp,sizeof(double));
            hc->update ( ((datamsg *)buf)->person, resp );
        }

        else if(*m==QUIT_MSG){
            chan->cwrite(m, sizeof(MESSAGE_TYPE));
            delete chan;
            break;
        }
        else if(*m==FILE_MSG){
            filemsg *fm = (filemsg*) buf;
            string fname = (char *)(fm+1);
            int sz = sizeof(filemsg) + fname.size() + 1;
            chan->cwrite( buf,sz);
            chan->cread(recvbuf,mb);

            string recvfname = "recv/" + fname;
            FILE* fp = fopen(recvfname.c_str(),"r+");
            fseek(fp, fm->offset,SEEK_SET);
            fwrite(recvbuf,1,fm->length,fp);
            fclose(fp);
        
        }

    }
    /*
		Functionality of the worker threads	
    */
}

void sig(int signal){
    system("clear");
    hc.print();
    alarm(2);
}



int main(int argc, char *argv[])
{
    
    int n = 1000;    //default number of requests per "patient"
    int p = 1;     // number of patients [1,15]
    int w = 200;    //default number of worker threads
    int b = 500; 	// default capacity of the request buffer, you should change this default
    int m = MAX_MESSAGE; 	// default capacity of the message buffer
    srand(time_t(NULL));
    string fname = "10.csv";
    bool isfile = false;
    bool isdatapoint = false;
    
    signal(SIGALRM, sig);
    alarm(2);
       
    int opt = -1;
        while((opt = getopt(argc,argv, "n:p:w:b:m:f:")) != -1){
        switch(opt){
            case 'm':
                m = atoi(optarg);
                break;
            case 'n':
                n = atof(optarg);
                break;
            case 'p':
                p = atoi(optarg);
                isdatapoint = true;
                break;                  
            case 'b':
                b = atoi(optarg);
                break; 
            case 'w':
                w = atoi(optarg);
                break;
            case 'f':
                fname = optarg;
                isfile =true;
                break;      
        }
    }
    cout<<"n: "<< n << " p: " << p << " w: " << w << " b: " << b <<endl;
    
    int pid = fork();
    if (pid == 0){
		// modify this to pass along m
        char *args[] = {"./server","-m",(char *) to_string(m).c_str(), NULL};
        execvp(args[0],args);
        
        if (execvp (args [0], args) < 0){
            perror ("exec filed");
            exit (0);
        } 
    }
    
	FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer (b);
	
	
    //making histogram and add to histogram collection  
	for(int i=0;i<p;i++){
        Histogram *h = new Histogram(10,-2.0, 2.0);
        hc.add(h);
    }
    cout<<"adding to histogram"<<endl;

    //making worker channels
    FIFORequestChannel* wchans [w];
	for(int i=0;i<w;i++){
        
        wchans[i] = create_new_channel(chan);

    }    
	cout<<"channel creation done"<<endl;

    struct timeval start, end;
    gettimeofday (&start, 0);

    /* Start all threads here */
    /*
    thread patient [p];
	for(int i=0;i<p;i++){
        patient[i] = thread(patient_thread_function, n,i+1,&request_buffer);
    } 
     cout<<"creating parent thread done"<<endl;
    */

    thread workers [w];
    for(int i=0;i<w;i++){
        workers[i] = thread(worker_thread_function, wchans[i], &request_buffer, &hc,m);
    }

    if(isfile == true){
        cout<<"it's a file transfer"<<endl;
        thread filethread (file_thread_function, fname, &request_buffer, chan, m);

        filethread.join();
        cout<<"file thread finished"<<endl;

        for(int i=0;i<w;i++){
            MESSAGE_TYPE m = QUIT_MSG;
            request_buffer.push( (char*)&m, sizeof(m) );
        }  

        for(int i=0;i<w;i++){
            workers[i].join();
        }
    }     

    else{
        cout<<"it's data point transfer"<<endl;
        thread patient [p];
        for(int i=0;i<p;i++){
            patient[i] = thread(patient_thread_function, n,i+1,&request_buffer);
        } 
        cout<<"creating parent thread done"<<endl;

        for(int i=0;i<p;i++){
            patient[i].join();
        }         
        cout<<"patient join done"<<endl;

        for(int i=0;i<w;i++){
            MESSAGE_TYPE m = QUIT_MSG;
            request_buffer.push( (char*)&m, sizeof(m) );
        }  

        for(int i=0;i<w;i++){
            workers[i].join();
        }
    }


  



    gettimeofday (&end, 0);
    // print the results
    system("clear");
	hc.print ();


    /*delete channels
    for(int i=0;i<p;i++){
            MESSAGE_TYPE q = QUIT_MSG;
            wchans[i]->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
            delete wchans[i];
    }*/


    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;


    // clean up channel
    MESSAGE_TYPE q = QUIT_MSG;
    chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    cout << "All Done!!!" << endl;
    delete chan;
    
}
