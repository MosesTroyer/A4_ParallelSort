#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>
#include <unistd.h>

#define USERLINE_SIZE 75
#define BUFFER_SIZE 1024

//Author: Moses Troyer 2014
//For Dr. Trenary's CS 2240, WMU

//Each line will be stored in a struct like this
struct userLine {
	char line[USERLINE_SIZE];
	int index;
};

//This will be passed to each pthread
struct threadData {
	char *fileName;
	int numberOfUsers;
	struct userLine users[500];
	pthread_t threadPointer;
};

//sorting function for qsort
int sortF(const void *a, const void *b){
	struct userLine *ua = (struct userLine *) a;
	struct userLine *ub = (struct userLine *) b;
	
	return ua->index - ub->index;
} //end sortF

//Thread for sorting each file
void *SortFile(void *passedPointer){
	struct threadData *td = (struct threadData *) passedPointer;
	char *fileName = (char *) td->fileName;
  FILE *f = fopen(fileName, "r");
	int numberOfUsers = 0, i = 0, j;

  char temp[BUFFER_SIZE];
  
  while(fgets(temp, BUFFER_SIZE, f))
		numberOfUsers++;

  rewind(f);

	struct userLine users[numberOfUsers];

  char *token;
	char *saveptr;
	for(i=0; i<numberOfUsers; i++){
		fgets(temp, BUFFER_SIZE, f);
		strcpy(users[i].line, temp);
	  token = strtok_r(temp, ",", &saveptr);
		for(j=0; j<4; j++){
			token = strtok_r(NULL, ",", &saveptr);
			if(j == 3){
				strncpy(temp, token, strlen(token) - 1);
				users[i].index = atoi(token);	
			}
		}
	}
	
  qsort(users, numberOfUsers, sizeof(* users), sortF);

  td->numberOfUsers = numberOfUsers;
  for(i=0;i<numberOfUsers;i++)
		td->users[i] = users[i];

  fclose(f);
	pthread_exit(NULL);
} //end SortFile

//For each file in data, spin up a process that uses qsort to sort the file
//Then, after all of them are sorted, the main process runs a merge sort
int main(int argc, char *argv[]){
  if(argc != 2) {
		printf("Please add a valid directory in the arguments!\nUsage: \"./Parallel <directory>\nThere is no '/' before the directory name.\n");
		return 0;
	}

  DIR *d;
  struct dirent *file;
	char *location = argv[1];
  int fileCount = 0;

	d = opendir(location);

	if(d == NULL){
		printf("Unable to open directory!\n");
		return 0;
	}

	chdir(location);

  while((file = readdir(d))){
		if(strcmp(file->d_name, ".") != 0 &&  
			 strcmp(file->d_name, "..") != 0) { //exclude . and ..

			fileCount++;
		}
	}

  rewinddir(d);

	struct threadData threads[fileCount];

	int i = 0;
	while((file = readdir(d))){
		if(strcmp(file->d_name, ".") != 0 && 
			 strcmp(file->d_name, "..") != 0) { //exclude . and ..
        
			threads[i].fileName = malloc(sizeof(strlen(file->d_name) + 1));
			strcpy(threads[i].fileName, file->d_name);

			//spinning up the threads
			pthread_create(&threads[i].threadPointer, NULL, SortFile, &threads[i]);
      	
			i++;
		} //end if		
	} //end while 

  for(i=0; i<fileCount; i++)
		pthread_join(threads[i].threadPointer, NULL);

	chdir("..");

  FILE *f = fopen("sorted.txt", "w"); 
  int positions[fileCount];
  int lowest, lowestFile;

	for(i=0;i<fileCount;i++)
		positions[i] = 0;

	int sorted = 1;
	while(sorted == 1){
    lowest = 2000000000;
		sorted = 0;
		for(i=0;i<fileCount;i++){
      if(positions[i] < threads[i].numberOfUsers){
				if(threads[i].users[positions[i]].index < lowest){
					lowest = threads[i].users[positions[i]].index;
					lowestFile = i;
					sorted = 1;
				}
			}

		} //end for
		if(sorted == 1) {
			fprintf(f, "%s", threads[lowestFile].users[positions[lowestFile]].line);
			positions[lowestFile]++;
		}
	} //end while
  
	fclose(f);
	pthread_exit(NULL);
} //end main
