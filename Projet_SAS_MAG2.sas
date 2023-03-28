
/*Réalisée par : Jean-Baptiste GOMEZ et Ulrich SEGODO*/
/***********************************PARTIE 1 : SAS BASE*************************************************/
/*1- Construire une macro fonction nommée << file_import >> qui permet d'importer une table il
doit contenir en paramètre :
- une macro variable qui contient le lien vers le dossier de stockage du fichier
- le nom du fichier et son extension
- le nom de la table en sortie
- le délimiter si nécessaire*/

%macro file_import(folder_path, file_name, table_name, delimiter=,);

   /* Création du chemin complet du fichier */
   %let full_path = %sysfunc(catx(/, &folder_path., &file_name.));

   /* Importation le fichier en utilisant la procédure IMPORT */
   proc import datafile="&full_path." out=&table_name. dbms=csv replace;
      delimiter=&delimiter.;
      getnames=yes;
   run;

%mend;

/*2- Utilisez la macro fonction créée dans la question précédente pour importer les 06 fichiers du
projet.*/

%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=customers.txt, 
 			 table_name=customers, delimiter='09'x);
 			 
%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=order_items.txt, 
 			 table_name=order_items, delimiter='09'x);
 			 
%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=order_payments.txt, 
 			 table_name=order_payments, delimiter='09'x);
 			 
%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=orders.txt, 
 			 table_name=orders, delimiter='09'x);
 			 
%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=products_translation.txt, 
 			 table_name=products_translation, delimiter='09'x);
 			 
%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=products.txt, 
 			 table_name=products, delimiter='09'x);
 
/* 3- A l’aide de Microsoft Excel ou de tout autre logiciel (indiquez le logiciel), construisez le
diagramme de la base de données qui relie toutes ces tables entre elles. Enregistrer le sous
format « PDF » et le joindre au dossier d’envoi.*/

/* 4- Dans une étape Data, créez une table nommée « customers1 » à partir de « customers ».
		- Ajoutez-y une nouvelle colonne nommée « anciennete » qui donne l’écart en mois entre la
		date de résiliation de la carte “cancellation_date” et celle de suscription.
		- une nouvelle colonne nommée "state_groupe" qui regroupe
		les modalité de la variable "customer_state" en 3 groupes comme suit :
		==> Groupe1 : les modalités commençants par A, B, C, D, E, F, G
		==> Groupe2 : les modalités commençants par M, N, O, P, Q
		==> Groupe3 : les modalités commençants par R, S, T, U, V*/
		
data WORK.customers1;
	set customers;
	attrib anciennete label="anciennete";
	anciennete = intck('month', card_date_subscription, cancellation_date);
	attrib state_groupe label="state_groupe";
	select (substr(customer_state, 1, 1));
      when ('A', 'B', 'C', 'D', 'E', 'F', 'G') state_groupe = 'Groupe1';
      when ('M', 'N', 'O', 'P', 'Q') state_groupe = 'Groupe2';
      when ('R', 'S', 'T', 'U', 'V') state_groupe = 'Groupe3';
      otherwise state_groupe = 'Other';
   end;	
run;

/*- Triez la table par « customer_state » et « anciennete ».*/
proc sort data=customers1;
   by customer_state anciennete;
run;

/*5)a- A l’aide d’une « étape PROC FREQ », donnez l’effectif des clients par “state_groupe” et
“anciennete”. Sauvegarder le résultat dans une table nommée « customers21 ».*/

proc freq data=customers1;
   tables state_groupe*anciennete/ out=customers21(drop=percent rename=(COUNT=n_contrat)) nocol norow nopercent;
run;

/*b- A l’aide d’une « étape PROC FREQ », donnez par “state_groupe” et “anciennete” le
nombre de clients ayant résilié (cancellation=1). Sauvegarder le résultat dans une table
nommée « customers22 ».*/

proc freq data=customers1(where = (cancellation = 1));
  tables state_groupe * anciennete/ out=customers22(drop=percent rename=(COUNT=n_resiliation)) nocol norow nopercent;
run;

/*c- A l’aide d’une « étape PROC FREQ », donnez l’effectif total des clients par “state_groupe”.
Sauvegarder le résultat dans une table nommée « customers23 »*/

proc freq data=customers1;
   tables state_groupe/ out=customers23(drop=percent rename=(COUNT=n_cohorte)) nocol norow nopercent;
run;

/* 6-
a- A l’aide d’une « étape DATA / merge » créez une table « customers31 » qui fusionne les
tables « customers21 » et « customers22 ». Dans cette même étape « DATA » créer une
nouvelle colonne qui cumule le nombre de contrats par “state_groupe” et par
“anciennete”*/

data customers31;
  merge customers22 customers21;

  /* Création d'une nouvelle variable qui cumule le nombre de contrats */
  retain cum_n_contrat 0;
  by state_groupe anciennete;
  if first.state_groupe and first.anciennete then cum_n_contrat = 0;
  cum_n_contrat = cum_n_contrat + n_contrat;
run;


/* b- A l’aide d’une « étape DATA / merge », créez une table « customers32 » qui fusionne les
tables « customers23 » et « customers31 ». Dans la même étape DATA, par
“state_groupe”, calculez :
	- Créez une colonne nommée “n_risque” par le calcul suivant
	n_risque = n_cohorte + n_contrat - cum_n_contrat
	- Créez une colonne nommée “tx_survie” par le calcul suivant
	tx_survie = LOG(1 - (n_resiliation/n_risque))
	- Créer une colonne nommée “estim”, initialisée à 0 pour chaque “state_groupe” et qui
	donne la somme cumulée du “tx_survie”
	- Créez une colonne nommée “ estimateur_survie” qui applique la fonction
	exponentielle à la colonne “estim”
*/
data customers32;
  merge customers31 customers23;
  
  /* Calcul des colonnes demandées */
  retain estim 0;
  by state_groupe;
  n_risque = n_cohorte + n_contrat - cum_n_contrat;
  if n_resiliation ne . then do;
    tx_survie = log(1 - (n_resiliation/n_risque));
  end;
  else do;
    tx_survie = 0;
  end;
  estim = estim + tx_survie;
  estimateur_survie = exp(estim);
  keep state_groupe anciennete n_resiliation n_contrat cum_n_contrat n_cohorte n_risque tx_survie estim estimateur_survie;
run;


/*7- A l’aide de Microsoft Excel, tracez sur un même graphique, les courbes qui donnent
l’estimateur de survie par ancienneté pour chaque groupe. La restitution doit se faire sur le
même fichier que la question 3.*/

/******************************** PARTIE 2 : SAS SQL *************************************/

/*
1- Écrivez le programme SAS qui permet d’obtenir le nombre distinct de clients par groupe
d'État et par type de carte. Ordonnez les résultats dans l’ordre décroissant suivant le nombre
de clients. Utiliser la variable « customer_id ».
*/

Proc sql;
	Select state_groupe, loyalty_card_type, COUNT(customer_id) 
	as total LABEL = "nombre de clients"
	from customers1
	group by state_groupe, loyalty_card_type
	order by total desc;
quit;

/*
2- En partant de la requête précédente, écrivez la requête qui permet d’obtenir le nombre de
commandes qui ont été passées au mois de juin 2017. Précisez également dans la même
requête par combien de clients ont-elles été passées. Affichez les résultats par état du
consommateur et type de carte de fidélité. Ordonnez les résultats dans l’ordre décroissant
suivant le nombre de clients.
*/

proc sql;
	Select
	Distinct(c.customer_state), c.loyalty_card_type,
	count(o.order_id) as t1 LABEL="nombre de commandes", 
	count(o.customer_id) as t2 LABEL="nombre de consommateurs" 
	from customers as c join orders as o
	on c.customer_id = o.customer_id
	group by c.customer_state, c.loyalty_card_type
	having MONTH(DATEPART(o.order_purchase_date))=6 
	and YEAR(DATEPART(o.order_purchase_date))=2017
	order by t1 desc
	;
quit;


/*
3- Écrivez le programme SAS qui permet d’obtenir pour les produits de poids supérieur 
à 29000, le nombre de commandes concernés. Afficher le résultat suivant le nom 
des produits en anglais.
*/

proc sql;
	select DISTINCT(pt.product_category_name_english), p.product_weight_g,
	COUNT( o.order_id) as total LABEL="Nombre de commandes"
	from products as p INNER JOIN order_items as o
	on p.product_id = o.product_id INNER JOIN products_translation as pt
	on p.product_category_name = pt.product_category_name
	where p.product_weight_g > 29000
	GROUP BY pt.product_category_name_english
	ORDER BY pt.product_category_name_english;
quit;

/*
4- Par type de carte de fidélité, affichez le nombre de commandes associées, 
chiffre d’affaires total des commandes, le minimum, moyen et maximum et 
l’écart type des montants de commandes.
*/

Proc sql;
	Select c.loyalty_card_type, COUNT(DISTINCT o.order_id) 
	as total LABEL = "NB_commandes",
	SUM(op.payment_value) as cf LABEL= "CA_total",
	MIN(op.payment_value) as m1 LABEL = "CA_min",
	MEAN(op.payment_value) as m2 LABEL = "CA_moy",
	MAX(op.payment_value) as m3 LABEL = "CA_max",
	STD(op.payment_value) as SD LABEL = "CA_sdt"
	from orders as o inner join customers as c
	on o.customer_id = c.customer_id 
	inner join order_payments as op on o.order_id = op.order_id
	group by c.loyalty_card_type;
quit;

/*
5- Écrivez la requête SQL qui permet de déterminer pour chaque de État, 
le chiffre d’affaires total, le nombre de commandes réalisées, 
le panier moyen et le nombre moyen d’UVC.
NB : Panier moyen = Chiffre d’affaires / nombre de commandes
Nombre moyen d’UVC = Nombre total de produits / nombre de commandes
*/
Proc sql;
	Select distinct c.customer_state,
	SUM(op.payment_value)  as cf LABEL= "CA_total",
    COUNT(DISTINCT o.order_id) AS Nb1 LABEL="NB_commandes",
    COUNT(Distinct c.customer_id) AS Nb2 LABEL="NB_clients",
	COUNT(oi.product_id) AS Nb3 LABEL="NB_produits",
	SUM(op.payment_value)/ COUNT(DISTINCT o.order_id) AS Panier_moyen,
   	COUNT(oi.product_id) / COUNT(DISTINCT o.order_id) AS NB_uvc
	from orders as o inner join customers as c
	on o.customer_id = c.customer_id 
	inner join order_payments as op on o.order_id = op.order_id
	inner join order_items as oi on o.order_id = oi.order_id
	group by c.customer_state
	order by cf desc;
quit;

/******************************** Partie 1 : SAS Macro *************************************/
/*
A-/ Sondage aléatoire simple (AS)
Chaque programme utilisera la table « customers » créée dans la partie SQL.
1- Programme AS1
Créez un programme SAS à l’aide d’une étape « Data », sans aucun macro-langage, qui :
créez une variable aléatoire nommée « i » en utilisant la fonction « ranuni (0) » de SAS.
Triez par cette variable « i » et créez un échantillon avec les 5000 premières observations.*/

/* Création d'un jeu de données avec une variable aléatoire */
data work.customers;
  do obs = 1 to 10000;
    i = ranuni(0);
    output;
  end;
run;
/* Trie des observations par la variable aléatoire */
proc sort data=work.customers;
  by i;
run;
/* Création d'un échantillon avec la taille spécifiée soit 5000 */
data work.AS1_sample;
  set work.customers (obs=5000);
run;





/*2- Programme AS2
Reprenez le programme AS1, toujours sans créer de macro-programme, ajouter en paramètre
(utilisez « %let ») le nom de la table en entrée et le nom de la table en sortie, ainsi que la
taille de l’échantillon (nombre d’observations).*/

/* Définissions des paramètres */
%let input_table = work.customers;
%let output_table = work.AS2_sample;
%let sample_size = 5000;
data &input_table.;
   do obs=1 to 10000;
      i = ranuni(0);
      output;
   end;
run;
proc sort data=&input_table.;
   by i;
run;
data &output_table.;
   set &input_table. (obs=&sample_size.);
run;





/*
3- Programme AS3
Reprenez le programme AS2 en remplaçant le paramètre nombre d’observation par le
pourcentage d’observation. Ainsi la valeur 20 de ce paramètre permettra d’obtenir un 
échantillon avec 20% des observations de la table d’entrée. Modifiez le programme en conséquence.
Attention : il vous faudra utiliser l’instruction « call symputx » et la macro-fonction
« %sysevalf ».*/

/* Définissions des paramètres */
%let input_table = work.customers;
%let output_table = work.AS3_sample;
%let sample_percent = 20;
/* Obtention du nombre total d'observations dans la table d'entrée */
proc sql noprint;
   select count(*) into :total_obs from &input_table.;
quit;
/* Calcule du nombre d'observations pour l'échantillon */
%let sample_size = %sysevalf((&sample_percent./100)*&total_obs.);
/* Création d'un jeu de données avec une variable aléatoire */
data &input_table.;
   do obs=1 to 10000;
      i = ranuni(0);
      output;
   end;
run;
proc sort data=&input_table;
   by i;
run;
/* Création d'un échantillon avec le pourcentage spécifié */
data &output_table.;
   set &input_table. (obs=%sysevalf(&sample_size.));
run;




/*
4- Programme AS4
Transformez le programme AS3 en un macro-programme « %AS », avec les trois paramètres :
table en entrée, table en sortie et taux d’échantillonnage.*/


%macro AS(input_table=, output_table=, sample_rate=);
   proc sql noprint;
      select count(*) into :total_obs from &input_table.;
   quit;
   %let sample_size = %sysevalf((&sample_rate./100)*&total_obs.);
   data &input_table;
      do obs=1 to 10000;
         i = ranuni(0);
         output;
      end;
   run;
   proc sort data=&input_table.;
      by i;
   run;
   data &output_table.;
      set &input_table. (obs=%sysevalf(&sample_size.));
   run;
%mend AS;

/* Testons la macro %AS */
%AS(input_table=work.customers, output_table=work.AS4_sample, sample_rate=20);



/*
B-/ Sondage aléatoire stratifié (ASTR)

Choisir dans la table « client_macro » une variable de stratification de type caractère (le type de carte
par exemple)

1- Programme ASTR1
Créez un macro-programme « %ASTR1 » qui permet de collecter dans des macro variables les
valeurs prises par la variable de stratification choisie, ainsi que leur effectif respectif.
Ce macro-programme aura comme paramètres : la table en entrée ainsi que la variable de
stratification.
Attention : Ne pas prendre en compte les valeurs manquantes pour la variable de
stratification.*/

%file_import(folder_path=/home/u63066564/sasuser.v94/GOMEZSAS,
 			 file_name=customers.txt, 
 			 table_name=customers, delimiter='09'x);
 			 
%macro ASTR1(input_table=, strat_var=);
  /* Supprimer les observations avec des valeurs manquantes pour la variable de stratification */
  data temp;
    set &input_table;
    where not missing(&strat_var);
  run;
  /* Collecter les valeurs de la variable de stratification et leur effectif respectif */
  proc sql noprint;
    select distinct &strat_var into :strat_vals separated by ' '
    from temp;
  quit;
  %let num_strat_vals = %sysfunc(countw(&strat_vals));
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    proc sql noprint;
      select count(*) into :obs_count
      from temp
      where &strat_var = "&strat_val";
    quit;
    %let obs_count_&strat_val = &obs_count;
  %end;
  /* Afficher les macro-variables créées */
  %put Les valeurs de la variable de stratification sont : &strat_vals.;
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    %put &strat_val: &&obs_count_&strat_val.;
  %end;
%mend ASTR1;

/* Testons la macro %ASTR1 */
%ASTR1(input_table=work.customers, strat_var=loyalty_card_type);
 



/*
2- Programme ASTR2
Reprenez le macro-programme « %ASTR1 » et ajoutez-y une partie qui éclate la table en
entrée en plusieurs strates : c’est-à-dire qu’il faut créer une table par strate.*/

%macro ASTR2(input_table=, strat_var=);
  /* Supprimer les observations avec des valeurs manquantes pour la variable de stratification */
  data temp;
    set &input_table;
    where not missing(&strat_var);
  run;
  /* Collecter les valeurs de la variable de stratification et leur effectif respectif */
  proc sql noprint;
    select distinct &strat_var into :strat_vals separated by ' '
    from temp;
  quit;
  %let num_strat_vals = %sysfunc(countw(&strat_vals));
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    /* Créer une table pour chaque strate */
    data strat_&strat_val;
      set &input_table;
      where &strat_var = "&strat_val";
    run;
    /* Collecter l'effectif de chaque strate dans une macro-variable */
    proc sql noprint;
      select count(*) into :obs_count
      from strat_&strat_val;
    quit;
    %let obs_count_&strat_val = &obs_count;
  %end;
  /* Afficher les macro-variables créées */
  %put Les valeurs de la variable de stratification sont : &strat_vals.;
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    %put &strat_val: &&obs_count_&strat_val.;
  %end;
%mend ASTR2;

/* Testons la macro %ASTR2 */
%ASTR2(input_table=WORK.customers, strat_var=loyalty_card_type)




/*
3- Programme ASTR3
Reprenez le programme « %ASTR2 » et adaptez le en ajoutant une partie qui crée les sous
échantillons (un échantillon pour chaque strate). Utilisez la fonction « ranuni (0) » de SAS en
vous inspirant du A-/
Attention : rajoutez dans les paramètres du macro-programme le taux d’échantillonnage et
adaptez votre programme en conséquence.*/

%macro ASTR3(input_table=, strat_var=, sample_rate=);
  /* Supprimer les observations avec des valeurs manquantes pour la variable de stratification */
  data temp;
    set &input_table;
    where not missing(&strat_var);
  run;
  /* Collecter les valeurs de la variable de stratification et leur effectif respectif */
  proc sql noprint;
    select distinct &strat_var into :strat_vals separated by ' '
    from temp;
  quit;
  %let num_strat_vals = %sysfunc(countw(&strat_vals));
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    /* Créer une table pour chaque strate */
    data strat_&strat_val;
      set &input_table;
      where &strat_var = "&strat_val";
    run;
    /* Collecter l'effectif de chaque strate dans une macro-variable */
    proc sql noprint;
      select count(*) into :obs_count
      from strat_&strat_val;
    quit;
    %let obs_count_&strat_val = &obs_count;
    /* Créer un sous-échantillon pour chaque strate */
    data samp_&strat_val;
      set strat_&strat_val;
      if ranuni(0) <= &sample_rate;
    run;
  %end;
  /* Afficher les macro-variables créées */
  %put Les valeurs de la variable de stratification sont : &strat_vals.;
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    %put &strat_val: &&obs_count_&strat_val.;
  %end;
%mend ASTR3;


/* Testons la macro %ASTR3 */
%ASTR3(input_table=WORK.customers, strat_var=loyalty_card_type,sample_rate =20)



/*
4- Programme ASTR4
Reprenez le programme « %ASTR3 » et ajoutez une partie qui joint les sous échantillons en
une seule table SAS.*/

%macro ASTR4(input_table=, strat_var=, sample_rate=, output_table=);
  /* Supprimer les observations avec des valeurs manquantes pour la variable de stratification */
  data temp;
    set &input_table;
    where not missing(&strat_var);
  run;
  /* Collecter les valeurs de la variable de stratification et leur effectif respectif */
  proc sql noprint;
    select distinct &strat_var into :strat_vals separated by ' '
    from temp;
  quit;
  %let num_strat_vals = %sysfunc(countw(&strat_vals));
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    /* Créer une table pour chaque strate */
    data strat_&strat_val;
      set &input_table;
      where &strat_var = "&strat_val";
    run;
    /* Collecter l'effectif de chaque strate dans une macro-variable */
    proc sql noprint;
      select count(*) into :obs_count
      from strat_&strat_val;
    quit;
    %let obs_count_&strat_val = &obs_count;
    /* Créer un sous-échantillon pour chaque strate */
    data samp_&strat_val;
      set strat_&strat_val;
      if ranuni(0) <= &sample_rate;
    run;
  %end;
  /* Joindre les sous-échantillons en une seule table SAS */
  data &output_table;
    set 
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    samp_&strat_val
  %end;
    ;
  run;
  /* Afficher les macro-variables créées */
  %put Les valeurs de la variable de stratification sont : &strat_vals.;
  %do i=1 %to &num_strat_vals;
    %let strat_val = %scan(&strat_vals, &i);
    %put &strat_val: &&obs_count_&strat_val.;
  %end;
%mend ASTR4;


/* Testons la macro %ASTR4 */
%ASTR4(input_table=WORK.customers, strat_var=loyalty_card_type, sample_rate = 20, output_table = finaltable)





