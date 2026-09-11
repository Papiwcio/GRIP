# Prompt 1 — Baza dowodów

Pełnisz funkcję asystenta badawczego wspierającego wielokrotne studium przypadku firm. Przygotuj ustrukturyzowaną bazę danych zastanych dotyczącą wskazanej firmy. **Nie twórz jeszcze końcowego raportu analitycznego.**

## Dane wejściowe

- **Nazwa firmy:**
- **KRS:**
- **NIP:**
- **REGON:**
- **Adres:**
- **Ewentualne wcześniejsze nazwy:**
- **Grupa kapitałowa:**
- **Okres badania:** 2019–2024

Jeżeli któregoś identyfikatora nie podano, nie zgaduj. Ustal go ze źródła formalnego albo oznacz jako brak danych.

## Cel

Zbierz, zweryfikuj i zakoduj publicznie dostępne informacje dotyczące firmy. Materiał ma służyć przygotowaniu pogłębionego studium przypadku i triangulacji z analizą ilościową.

Analiza obejmuje siedem obszarów:

1. zdolności dynamiczne;
2. cyfryzacja;
3. umiędzynarodowienie;
4. doświadczenie w zarządzaniu kryzysowym;
5. przejawy przywództwa;
6. kultura organizacyjna;
7. wyniki ekonomiczno-finansowe.

## Rama czasowa

Stosuj wyłącznie następujące okresy:

- **KONTEKST WYJŚCIOWY:** sytuacja firmy na wejściu w rok 2019;
- **P1:** 2019–2020;
- **P2:** 2020–2022;
- **P3:** 2022–2024;
- **MATERIAŁ PÓŹNIEJSZY:** źródła lub zdarzenia od 2025 roku.

Nie nazywaj kontekstu wyjściowego P1. Nie twórz P4. Informacji dotyczących lat 2025–2026 nie przypisuj do P3.

Kontekst wyjściowy nie oznacza pełnej historii firmy. Systematycznie analizuj przede wszystkim lata 2015–2018. Wydarzenia z lat 2010–2014 uwzględniaj tylko wtedy, gdy miały trwałe znaczenie dla sytuacji firmy w 2019 roku. Wydarzenia sprzed 2010 roku uwzględniaj wyjątkowo, jeżeli miały charakter formacyjny.

Data publikacji źródła i data zdarzenia są odrębnymi informacjami. Późniejsze źródło może dokumentować wcześniejsze zdarzenie, ale materiał opisujący nowy stan od 2025 roku pozostaje materiałem późniejszym.

## Identyfikacja podmiotu

Przed rozpoczęciem analizy jednoznacznie potwierdź podmiot na podstawie nazwy, KRS, NIP, REGON, adresu i struktury właścicielskiej. Zapisz wynik identyfikacji w metadanych.

Nie mieszaj danych badanego podmiotu z:

- innymi spółkami o podobnej nazwie;
- jednostką dominującą;
- innymi spółkami grupy;
- podmiotami zagranicznymi grupy;
- przedsiębiorstwami Philips lub Phillips, które nie mają tego samego KRS.

Informacje o grupie można wykorzystać wyłącznie wtedy, gdy źródło jednoznacznie odnosi je również do badanego podmiotu albo gdy są potrzebne do opisania kontekstu właścicielskiego. W takim przypadku jasno oznacz poziom analizy.

## Protokół wyszukiwania źródeł

Kwerenda ma być systematyczna, iteracyjna i nastawiona na odkrywanie tropów, a nie opportunistyczna. Nie kończ pracy po znalezieniu kilku źródeł ogólnych. Każde istotne nazwisko, klient, produkt, system, stanowisko, rynek, ryzyko finansowe lub nagroda ujawnione w jednym źródle potraktuj jako nowy trop do osobnej kwerendy.

Pracuj w czterech fazach:

1. **Identyfikacja i mapa nazw:** potwierdź podmiot, wcześniejsze nazwy, grupę, adres, osoby, produkty, klientów, rynki i słowa branżowe.
2. **Kwerenda rodzin źródeł A-H:** przejdź przez wszystkie rodziny źródeł opisane niżej.
3. **Kwerenda po tropach:** wykonaj dodatkowe zapytania po nazwiskach, klientach, produktach, systemach, stanowiskach, ryzykach finansowych i źródłach odkrytych w fazie 2.
4. **Audyt zamknięcia:** przed zapisaniem skoroszytu sprawdź listę kontrolną `Audyt domknięcia kwerendy` i odnotuj wynik w `07_KONTROLA_JAKOSCI`.

Przed zapisaniem bazy odnotuj w `01_REJESTR_ZRODEL`, które rodziny źródeł sprawdzono, z jakim wynikiem, jakich wariantów nazwy użyto oraz jakie tropy wynikły z każdego źródła.

Jeżeli badacz dostarczy raport benchmarkowy, raport poprzedniej rundy albo inną listę tropów źródłowych, wykonaj dodatkową fazę **backtestu źródłowego**:

1. wyodrębnij z benchmarku wszystkie adresy URL, nazwy źródeł, osoby, klientów, stanowiska, produkty, systemy, ryzyka finansowe i liczby;
2. dla każdego tropu wykonaj niezależną kwerendę źródła pierwotnego lub najbliższego dostępnego źródła;
3. w `01_REJESTR_ZRODEL` oznacz każdy trop statusem: `włączony`, `już pokryty`, `zastąpiony lepszym źródłem`, `duplikat`, `nieistotny dla badanego KRS`, `brak dostępu` albo `niepotwierdzony`;
4. nie kopiuj twierdzeń z benchmarku do bazy bez ponownego sprawdzenia źródła;
5. jeżeli benchmark wskazuje ważne twierdzenie, którego nie udało się potwierdzić, utwórz wiersz `G. BRAK DANYCH` albo `E. NIEZWERYFIKOWANE`, a nie fakt;
6. w `07_KONTROLA_JAKOSCI` podaj liczbę tropów benchmarkowych, liczbę pokrytych oraz listę nadal niepokrytych tropów krytycznych.

Backtest benchmarku nie zastępuje własnej kwerendy A-H. Służy do testowania czułości silnika wyszukiwania i musi poprawiać procedurę, a nie przepisywać cudzy raport.

Arkusz `01_REJESTR_ZRODEL` powinien mieć co najmniej następujące kolumny:

1. `id_zrodla`;
2. `rodzina_zrodla`;
3. `nazwa_zrodla`;
4. `tytul_lub_zakres`;
5. `data_publikacji_lub_dostepu`;
6. `typ_zrodla`;
7. `charakter_zrodla`;
8. `niezaleznosc`;
9. `wiarygodnosc`;
10. `adres_url`;
11. `warianty_zapytan_lub_sciezka_dotarcia`;
12. `wynik_kwerendy`;
13. `tropy_do_dalszej_kwerendy`;
14. `ograniczenia`.

Jeżeli źródło nie przyniosło danych, również wpisz je do rejestru jako wynik negatywny. Rejestr źródeł ma być mapą wykonanej pracy, nie tylko bibliografią wykorzystanych materiałów.

### A. Źródła formalne i rejestrowe

- KRS, MSiG, Repozytorium Dokumentów Finansowych KRS, sprawozdania finansowe i sprawozdania zarządu;
- rejestry osób i zmian organów, w tym źródła pokazujące historię zarządu i właścicieli;
- bazy agregujące dane rejestrowe wyłącznie jako trop lub źródło pomocnicze, chyba że wskazują dokument pierwotny.
- sprawdź także strony osób lub zdarzeń rejestrowych, jeżeli agregator prowadzi do profili członków zarządu, wspólników, beneficjentów, powiązań lub dokumentów;
- wyszukaj poprzednie nazwy spółki, jeżeli zmiana nazwy nastąpiła w okresie badania albo bezpośrednio przed nim;
- jeżeli źródło formalne pokazuje dostępność dokumentu, ale dokument nie został pobrany lub odczytany, zapisz osobny wiersz braku danych z konkretnym dokumentem do pozyskania.

### B. Źródła finansowe i operacyjne

- dokumenty RDF, sprawozdania, noty, opinie biegłych, informacje o kredytach, kowenantach, kontynuacji działalności i zdarzeniach po dniu bilansowym;
- wiarygodne bazy finansowe, jeżeli podają pierwotne źródło i zakres danych;
- portale agregujące wartości liczbowe, ale tylko z wyraźnym ograniczeniem interpretacyjnym, jeżeli nie dotarto do dokumentu źródłowego.
- obowiązkowo szukaj czerwonych flag finansowych: `strata`, `utrata`, `kowenant`, `naruszenie kowenantów`, `linia kredytowa`, `kredyt`, `leasing`, `zobowiązania`, `płynność`, `kontynuacja działalności`, `działania naprawcze`, `zdarzenia po dniu bilansowym`, `opinia biegłego`, `zastrzeżenie`, `transfer pricing`, `ceny transferowe`;
- dla lat ze stratą, spadkiem przychodów lub rozbieżnością agregatorów zapisz osobne wiersze: samą liczbę, ograniczenie źródła, możliwe wyjaśnienia do sprawdzenia oraz pytanie do wywiadu;
- nie oznaczaj liczby finansowej jako `A. DANE FORMALNE`, jeżeli pochodzi wyłącznie z agregatora, nawet gdy agregator powołuje się na KRS.

### C. Źródła branżowe, prasowe i targowe

- portale branżowe, wywiady, komunikaty targowe, informacje o produktach, klientach, magazynach, inwestycjach, ekspansji geograficznej i certyfikacji;
- źródła zewnętrzne i oficjalne rozdzielaj według faktycznej genezy treści. Artykuł oparty na komunikacie firmy nie jest niezależnym potwierdzeniem.
- szukaj kombinacji nazwy spółki z nazwami produktów, kategorii, targów, klientów i kanałów: `OEM`, `OE`, `aftermarket`, `eksport`, `rynków`, `Daimler`, `Mercedes`, `KAM`, `Key Account`, `DACH`, `EMEA`, `flota`, `dystrybutor`, `heavy-duty`, `naczepy`, `wiązki`, `24V`;
- jeżeli pojawia się konkretna liczba handlowa, np. udział eksportu, liczba rynków, udział kanału, największy klient lub segment, utwórz osobny wiersz dowodowy z ograniczeniem, nawet jeśli źródło jest branżowe;
- rozdziel: potwierdzony fakt publikacji, cytowaną narrację menedżera, opis redakcyjny oraz interpretację dotyczącą strategii.

### D. Źródła osobowe i przywódcze

- profile zarządu, dyrektorów i kluczowych menedżerów;
- wywiady, biogramy, strony konferencji, rankingi branżowe i zawodowe;
- rejestruj osobno fakt pełnienia funkcji, narrację osoby oraz interpretację dotyczącą stylu przywództwa.
- dla każdej osoby ujawnionej w źródłach wykonaj kwerendę po imieniu i nazwisku łączonym z nazwą spółki, poprzednią nazwą spółki, grupą kapitałową, stanowiskiem oraz słowami `wywiad`, `konferencja`, `nagroda`, `LinkedIn`, `TheOrg`, `Women in Law`, `Legal 500`, `Forum Biznesu`;
- odróżniaj funkcję formalną w KRS od funkcji operacyjnej, regionalnej lub komunikacyjnej, np. `prezes zarządu`, `CEO`, `President`, `General Manager`, `Vice President`, `Sales Director`;
- jeżeli źródła różnie datują lub nazywają funkcje, zapisz konflikt zamiast wybierać jedną wersję.

### E. Źródła zatrudnienia, kultury i kompetencji

- oferty pracy, profile pracodawcy, portale rekrutacyjne, informacje o benefitach, wynagrodzeniach, certyfikatach, systemach jakości, B+R, narzędziach CAD/CAM, ERP/MES/WMS/BI i kompetencjach organizacyjnych;
- pojedyncza oferta pracy potwierdza zapotrzebowanie lub deklarowany zakres stanowiska w dniu publikacji, ale nie dowodzi stabilnego stanu organizacji w całym okresie.
- sprawdź portale i archiwa ofert pracy: Pracuj.pl, Aplikuj.pl, LinkedIn Jobs, GoWork, RocketJobs, Indeed, OLX Praca oraz strony kariery firmy;
- szukaj kompetencji i systemów przez nazwy stanowisk oraz narzędzia: `kierownik B+R`, `R&D manager`, `quality manager`, `IATF`, `ISO`, `CAD`, `CAM`, `SolidWorks`, `ERP`, `MES`, `WMS`, `BI`, `EDI`, `lean`, `TPM`, `utrzymanie ruchu`, `narzędziownia`, `wtrysk`, `ekstruzja`;
- zapisuj osobno: istnienie stanowiska, deklarowany zakres obowiązków, wymagane narzędzia/systemy, lokalizację, datę publikacji oraz ograniczenie reprezentatywności.

### F. Źródła pracownicze i reputacyjne

- portale opinii pracowniczych i społecznościowe traktuj ostrożnie: zapisuj realne wypowiedzi jako wypowiedzi, a nie jako reprezentatywny fakt o organizacji;
- odróżniaj opinie historyczne, aktualne i syntetyczne lub wygenerowane automatycznie. Jeżeli autentyczność lub data są niejasne, zaznacz ograniczenie.
- odróżniaj dane z ogłoszeń, deklaracje pracowników, automatyczne podsumowania portalu i pojedyncze anonimowe wpisy;
- jeżeli portal podaje liczbę deklaracji, datę wpisu, stanowisko, widełki płacowe albo benefit, zapisz te elementy w ograniczeniach interpretacyjnych;
- pojedyncze wypowiedzi pracownicze mogą generować pytania do wywiadu, ale nie mogą same dowodzić kultury organizacyjnej.

### G. Źródła mediów społecznościowych i stron aktualnych

- LinkedIn, strony firmowe, strony produktowe i profile osób mogą ujawniać produkty, role, regiony sprzedaży, rekrutacje i późniejsze narracje;
- nie przypisuj automatycznie aktualnego stanu strony do lat 2019–2024. Jeżeli brakuje historycznej migawki, oznacz materiał jako `MATERIAŁ PÓŹNIEJSZY` albo wpisz ograniczenie datowania.
- sprawdź oficjalne podstrony typu `about`, `management`, `people`, `careers`, `news`, `certificates`, `products`, `rules and regulations`, `downloads`;
- w mediach społecznościowych szukaj nie tylko profilu firmy, lecz także postów osób, rekrutacji, targów, produktów, klientów, zdjęć z wydarzeń i komentarzy wskazujących datę;
- aktualne strony traktuj jako trop do historycznej weryfikacji, chyba że jasno dokumentują zdarzenie z okresu badania.

### H. Kwerenda negatywna

Dla każdego z siedmiu obszarów odnotuj także istotne braki: czego szukano i czego nie znaleziono. Brak danych publicznych zapisuj jako `G. BRAK DANYCH`, a nie jako brak zjawiska.

### Minimalne warianty zapytań

Szukaj co najmniej po:

- aktualnej nazwie;
- wcześniejszych nazwach;
- KRS, NIP i REGON;
- adresie lub miejscowości;
- nazwie grupy kapitałowej połączonej z nazwą spółki;
- nazwiskach członków zarządu i kluczowych menedżerów;
- słowach kluczowych dla obszarów: eksport, OEM, R&D, B+R, patent, certyfikat, IATF, ISO, ERP, MES, WMS, e-commerce, COVID, kryzys, kredyt, kowenant, magazyn, inwestycja, praca, wynagrodzenie, pracodawca.

Nie wpisuj do bazy twierdzeń tylko dlatego, że brzmią prawdopodobnie albo pojawiły się w notatkach pomocniczych. Każde twierdzenie musi mieć źródło, status `E. NIEZWERYFIKOWANE` albo status `G. BRAK DANYCH`.

### Kwerenda po tropach

Po przejściu przez minimalne zapytania wykonaj drugą rundę kwerendy po tropach ujawnionych w źródłach. Nie zakładaj, że pierwsze źródło wyczerpuje temat.

Obowiązkowo utwórz listę tropów i sprawdź:

- **osoby:** członkowie zarządu, dyrektorzy sprzedaży, dyrektorzy operacyjni, HR/ESG, R&D/B+R, jakość, finanse;
- **klienci i kanały:** nazwy klientów, OEM/OE, aftermarket, flota, dystrybutorzy, segmenty branżowe, udział eksportu, liczba rynków, regiony;
- **produkty i technologie:** nazwy produktów, patenty, certyfikaty, CAD/CAM, ERP, MES, WMS, BI, EDI, automatyzacja, R&D/B+R;
- **finanse i ryzyka:** lata strat, spadki przychodów, kowenanty, kredyty, leasing, płynność, kontynuacja działalności, transfer pricing, zdarzenia po dniu bilansowym;
- **praca i kultura:** oferty pracy, profile pracodawcy, benefity, wynagrodzenia, opinie pracownicze, nagrody pracodawcy, BHP, rotacja, absencja;
- **targi i media branżowe:** targi, konferencje, premiery produktów, relacje z fabryk, wywiady z menedżerami, artykuły w prasie branżowej.

Dla każdego tropu w `01_REJESTR_ZRODEL` wpisz, czy został:

- `potwierdzony`;
- `częściowo potwierdzony`;
- `niepotwierdzony`;
- `sprzeczny`;
- `brak dostępu`.

Tropy `niepotwierdzony`, `sprzeczny` i `brak dostępu` powinny trafić także do `03_NIEZWERYFIKOWANE`, `04_KONFLIKTY` albo `05_BRAKI`, zależnie od charakteru problemu.

### Audyt domknięcia kwerendy

Przed zapisaniem skoroszytu wykonaj końcowy audyt porównywalny z checklistą źródłową. W `07_KONTROLA_JAKOSCI` zapisz status każdej pozycji jako `wykonano`, `brak danych`, `nie dotyczy` albo `wymaga ręcznego dostępu`.

Audyt musi obejmować:

1. czy pobrano lub odnotowano brak dostępu do pełnych dokumentów RDF dla wszystkich lat okresu badania;
2. czy sprawdzono czerwone flagi finansowe: kowenanty, linie kredytowe, kontynuację działalności, leasing, zdarzenia po dniu bilansowym i działania naprawcze;
3. czy sprawdzono poprzednie nazwy spółki i podmiotów przejętych;
4. czy sprawdzono osoby z zarządu i co najmniej dwóch menedżerów operacyjnych lub handlowych, jeżeli ich nazwiska są publicznie dostępne;
5. czy sprawdzono źródła branżowe po produktach, klientach, kanałach i regionach sprzedaży;
6. czy sprawdzono oferty pracy i profile pracodawcy pod kątem systemów, kompetencji i kultury;
7. czy sprawdzono oficjalne podstrony firmy, w tym aktualności, certyfikaty, zarząd/ludzi, produkty, karierę i regulaminy;
8. czy sprawdzono co najmniej jedno źródło społecznościowe lub zawodowe firmy oraz kluczowych osób;
9. czy odnotowano negatywne wyniki dla każdego z siedmiu obszarów;
10. czy każde twierdzenie z materiałów późniejszych ma oddzielnie zakodowaną datę publikacji i datę zdarzenia;
11. czy lista braków zawiera nie tylko `brak danych`, ale także konkretne dokumenty, osoby lub pytania potrzebne do weryfikacji.
12. jeżeli istnieje benchmark lub poprzednia runda: czy wykonano backtest źródłowy, pokryto wszystkie źródła benchmarkowe albo jawnie wyjaśniono ich brak, duplikację, nieistotność lub niedostępność.

### Kryterium dramatycznej przewagi nad benchmarkiem

Jeżeli celem przebiegu jest przebicie konkretnego benchmarku, uznaj wynik za dramatycznie lepszy dopiero wtedy, gdy spełnia wszystkie warunki:

- rejestr źródeł i ścieżek kwerendy ma co najmniej 2 razy więcej pozycji niż benchmark albo, przy małej dostępności publicznej, zawiera wszystkie źródła benchmarku plus co najmniej 10 dodatkowych niezależnych lub istotnie różnych ścieżek;
- żadne źródło z benchmarku nie pozostaje bez statusu backtestu;
- baza ma więcej pojedynczych, audytowalnych twierdzeń niż benchmark i zachowuje zasadę jeden wiersz = jedno twierdzenie;
- każdy z siedmiu obszarów ma dodatnie dowody albo jawny brak danych;
- finanse, klienci/kanały/eksport, osoby, oferty pracy/systemy i źródła negatywne są sprawdzone osobno;
- ograniczenia, konflikty i braki są liczniejsze lub bardziej precyzyjne niż w benchmarku, bez przedstawiania hipotez jako faktów.

## Rodzaje materiału

Każdy wiersz przypisz do jednej kategorii:

- fakt lub zdarzenie;
- dane liczbowe;
- narracja oficjalna;
- narracja menedżerska;
- narracja zewnętrzna;
- opinia pracownicza;
- interpretacja ekspercka.

Nie przedstawiaj opinii, deklaracji ani materiałów promocyjnych jako faktów.

## Jednostka zapisu

Jedno niezależne twierdzenie lub obserwacja musi stanowić jeden wiersz. Nie łącz kilku zdarzeń, źródeł lub ocen w jednym wierszu. Jeżeli jedno źródło zawiera trzy odrębne informacje, utwórz trzy wiersze.

Każdy wiersz musi mieć unikalne i stabilne ID. Nie zmieniaj istniejących ID podczas późniejszej korekty bazy.

ID informacji powinny używać prefiksu właściwego dla analizowanej firmy, np. `PE-001` dla Phillips Europe albo innego krótkiego kodu utworzonego z nazwy firmy. Nie przenoś prefiksu `PE` na inną firmę. Jeżeli badacz poda preferowany prefiks, użyj go konsekwentnie w arkuszach `02_BAZA_DOWODOW`, `03_NIEZWERYFIKOWANE`, `04_KONFLIKTY`, `06_PYTANIA` i w raporcie Promptu 2.

Jeżeli poprawiasz lub migrujesz istniejącą bazę, nie powtarzaj wyszukiwania źródeł bez osobnej decyzji badacza. Najpierw dostosuj strukturę skoroszytu do aktualnego Promptu 1, zachowaj istniejące ID, napraw przesunięcia wartości między kolumnami i odnotuj ograniczenia w polach merytorycznych, a nie w polach zarezerwowanych dla badacza.

## Format wyniku

Utwórz jeden skoroszyt Excel i zapisz go bezpośrednio w folderze `Outputs/`. Nie twórz podfolderów dla pojedynczej firmy i nie zostawiaj w folderze `Outputs/` plików pomocniczych, podglądów, logów, kopii roboczych ani wariantów roboczych.

Nazwa pliku ma mieć format:

- `[Nazwa_firmy_bez_formy_prawnej]_Baza_dowodow.xlsx`

Przykłady:

- `Phillips_Europe_Baza_dowodow.xlsx`;
- `Nazwa_Innej_Firmy_Baza_dowodow.xlsx`.

W nazwie pliku stosuj krótką nazwę firmy, bez `Sp. z o.o.`, `S.A.`, znaków interpunkcyjnych i dopisków typu `Prompt_1`, `Robust`, `final`, `v2` lub daty. Kolejne przebiegi dla tej samej firmy nadpisują roboczy plik w `Outputs/`, chyba że badacz wyraźnie poprosi o zachowanie wariantów.

Skoroszyt musi zawierać osobne arkusze:

1. `00_METADANE`
2. `01_REJESTR_ZRODEL`
3. `02_BAZA_DOWODOW`
4. `03_NIEZWERYFIKOWANE`
5. `04_KONFLIKTY`
6. `05_BRAKI`
7. `06_PYTANIA`
8. `07_KONTROLA_JAKOSCI`

Arkusze 03–06 powinny być tworzone z pól bazy dowodów lub odwoływać się do ID wierszy źródłowych. Nie przepisuj treści w sposób, który utrudni późniejsze uzgodnienie zmian.

## Formatowanie skoroszytu

Skoroszyt ma być wygodny do przeglądu badacza:

- w każdym arkuszu zamroź pierwszy wiersz i pierwszą kolumnę;
- w każdym arkuszu włącz filtr dla całego użytego zakresu;
- pierwszy wiersz każdego arkusza sformatuj jako widoczny wiersz tytułowy: pogrubiona czcionka, kontrastowy kolor tła i czytelny kolor tekstu;
- nie zostawiaj białego tekstu na białym tle ani innych nagłówków niewidocznych po otwarciu w Excelu;
- dopasuj szerokości kolumn tak, aby nagłówki i krótkie pola kontrolne były czytelne, a dłuższe opisy miały zawijanie tekstu.

## Metadane i bramka przeglądu

W arkuszu `00_METADANE` umieść identyfikatory firmy, datę utworzenia bazy, zakres wyszukiwania oraz pole:

- **status_przegladu_badacza** — dozwolone wartości: `nierozpoczęty`, `w toku`, `zakończony`.

Nowa baza musi mieć wartość `nierozpoczęty`. AI nie może samodzielnie zmienić jej na `zakończony`. Może to zrobić wyłącznie badacz po zakończeniu przeglądu całej bazy.

Dodaj również pola opcjonalne:

- `badacz`;
- `data_zakonczenia_przegladu`;
- `uwagi_do_przegladu`.

## Kolumny bazy dowodów

W arkuszu `02_BAZA_DOWODOW` utwórz dokładnie następujące kolumny, w podanej kolejności:

1. `id_informacji`
2. `nazwa_firmy`
3. `krs`
4. `obszar_analizy`
5. `podkategoria`
6. `okres_analityczny`
7. `data_rozpoczecia_zdarzenia`
8. `data_zakonczenia_zdarzenia`
9. `data_publikacji_zrodla`
10. `rodzaj_materialu`
11. `neutralne_twierdzenie`
12. `cytat_lub_dokladne_streszczenie`
13. `nazwa_zrodla`
14. `tytul_dokumentu_lub_publikacji`
15. `adres_zrodla`
16. `typ_zrodla`
17. `charakter_zrodla`
18. `niezaleznosc_od_firmy`
19. `wiarygodnosc_zrodla`
20. `status_weryfikacji`
21. `liczba_niezaleznych_potwierdzen`
22. `sila_dowodu`
23. `kierunek_znaczenia`
24. `ton_narracji`
25. `mozliwy_konflikt`
26. `id_informacji_w_konflikcie`
27. `ograniczenia_interpretacyjne`
28. `propozycja_ai`
29. `wyjatek_badacza`
30. `komentarz_badacza`
31. `pytanie_do_wywiadu`

AI wypełnia kolumny 1–28 i 31. Kolumny `wyjatek_badacza` oraz `komentarz_badacza` pozostawia puste.

W bazach migrowanych ze starszej struktury dawne rekomendacje typu `zaakceptować` lub `do weryfikacji` należy przepisać do `propozycja_ai` jako odpowiednio `wykorzystać` albo `zweryfikować`. Dawne komentarze AI dotyczące ograniczeń, źródeł lub weryfikacji należy przenieść do `ograniczenia_interpretacyjne`; nie wolno ich pozostawiać w `komentarz_badacza`.

### Dozwolone wartości pól kontrolowanych

- `charakter_zrodla`: `oficjalne`, `zewnętrzne`;
- `niezaleznosc_od_firmy`: `wysoka`, `średnia`, `niska`;
- `wiarygodnosc_zrodla`: `wysoka`, `średnia`, `niska`;
- `sila_dowodu`: `0`, `1`, `2`, `3`;
- `kierunek_znaczenia`: `pozytywny`, `negatywny`, `mieszany`, `niejasny`;
- `mozliwy_konflikt`: `tak`, `nie`;
- `propozycja_ai`: `wykorzystać`, `odrzucić`, `zweryfikować`, `poprawić`;
- `wyjatek_badacza`: puste, `odrzuć`, `popraw`, `zweryfikuj`.

`propozycja_ai` jest rekomendacją pomocniczą i nie zastępuje decyzji badacza. Puste `wyjatek_badacza` nie oznacza akceptacji, dopóki `status_przegladu_badacza` nie ma wartości `zakończony`.

Po zakończeniu przeglądu:

- puste `wyjatek_badacza` oznacza przyjęcie wiersza do raportu;
- `odrzuć` oznacza wyłączenie wiersza;
- `popraw` oznacza wyłączenie do czasu wprowadzenia korekty i usunięcia wyjątku przez badacza;
- `zweryfikuj` oznacza wyłączenie do czasu weryfikacji i usunięcia wyjątku przez badacza.

## Status weryfikacji

Stosuj dokładnie następujące statusy:

### A. DANE FORMALNE

Informacja pochodzi bezpośrednio ze sprawozdania finansowego, KRS, MSiG, dokumentu regulacyjnego lub innego źródła formalnego.

### B. TREŚĆ ŹRÓDŁA POTWIERDZONA

Źródło istnieje i rzeczywiście zawiera wskazane twierdzenie, ale twierdzenie nie zostało niezależnie potwierdzone.

### C. POTWIERDZONE NIEZALEŻNIE

To samo twierdzenie potwierdzają co najmniej dwa niezależne źródła.

### D. NARRACJA WŁASNA FIRMY

Potwierdzono, że firma lub jej przedstawiciel przekazali taką informację, ale nie potwierdzono niezależnie jej zgodności z rzeczywistością.

### E. NIEZWERYFIKOWANE

Nie znaleziono źródła albo źródło nie zawiera wskazanego twierdzenia.

### F. SPRZECZNE

Wiarygodne źródła podają odmienne informacje.

### G. BRAK DANYCH

Po przeprowadzeniu zdefiniowanego wyszukiwania nie odnaleziono informacji.

## Zasady oceny

1. Znalezienie publikacji nie oznacza niezależnego potwierdzenia twierdzenia.
2. Materiał branżowy oparty na komunikacie prasowym firmy klasyfikuj jako narrację oficjalną lub źródło o niskiej niezależności.
3. Nagroda pracodawcy potwierdza fakt otrzymania nagrody, ale nie dowodzi sama w sobie wysokiej jakości kultury organizacyjnej.
4. Oferta pracy może potwierdzać istnienie stanowiska, systemu lub kompetencji w dniu publikacji, lecz nie dowodzi ich istnienia we wcześniejszych latach.
5. Pojedyncza anonimowa opinia pracownika jest realną wypowiedzią, ale ma niską wagę i nie reprezentuje całej organizacji.
6. Data publikacji źródła nie jest automatycznie datą zdarzenia.
7. Korelacja czasowa nie stanowi dowodu związku przyczynowego.
8. Brak danych nie oznacza braku badanego zjawiska.
9. Nie uzupełniaj luk prawdopodobnymi informacjami.
10. Każde twierdzenie musi mieć źródło albo status `E. NIEZWERYFIKOWANE` lub `G. BRAK DANYCH`.
11. Dwa źródła powtarzające ten sam komunikat firmy nie są dwoma niezależnymi potwierdzeniami.
12. Nie podwyższaj siły dowodu wyłącznie z powodu liczby publikacji zależnych od jednego źródła pierwotnego.
13. Jeżeli źródło późniejsze zawiera retrospektywną informację o zdarzeniu z okresu badania, koduj datę publikacji i datę zdarzenia oddzielnie oraz wyjaśnij, czy źródło dokumentuje zdarzenie historyczne, czy tylko aktualną narrację.
14. Dla twierdzeń o kompetencjach organizacyjnych, cyfryzacji, B+R, kulturze lub odporności kryzysowej preferuj wiele typów źródeł: dokument formalny, źródło branżowe, ofertę pracy, wypowiedź menedżerską i dane pracownicze. Jeżeli dostępny jest tylko jeden typ źródła, zaznacz ograniczenie.

## Dane finansowe

Dane finansowe pozyskuj przede wszystkim z Repozytorium Dokumentów Finansowych KRS, sprawozdań finansowych, sprawozdań zarządu, dokumentów audytowanych oraz wiarygodnych baz wskazujących pierwotne źródło.

Dla każdej liczby podaj rok obrotowy, jednostkę, rodzaj wyniku, dokładny dokument źródłowy oraz stronę lub notę, jeżeli jest dostępna. Umieść te elementy w neutralnym twierdzeniu, dokładnym streszczeniu lub ograniczeniach interpretacyjnych, zależnie od charakteru danych.

Nie zapisuj źródła jako `portal/KRS`. Wskaż dokładnie, skąd pochodzi liczba. Jeżeli nie można dotrzeć do dokumentu pierwotnego, oznacz ograniczenie oraz ustaw `propozycja_ai = zweryfikować`; nie przedstawiaj liczby jako danych formalnych.

Jeżeli dostępne są sprawozdania zarządu lub informacje dodatkowe, wyodrębnij osobno:

- przychody, wynik brutto, wynik netto, aktywa, kapitał własny, zobowiązania i zatrudnienie;
- wyjaśnienia zarządu dotyczące zmian wyników;
- informacje o istotnych klientach, rynkach, kanałach, ryzykach dostaw, ryzykach walutowych i cenach transferowych;
- kredyty, leasing, linie finansowania, naruszenia kowenantów, działania naprawcze i ocenę kontynuacji działalności;
- zdarzenia po dniu bilansowym;
- informację, czy dokument był badany przez biegłego rewidenta i czy opinia zawierała zastrzeżenia.

Jeżeli dokument formalny ujawnia epizod kryzysowy, np. naruszenie kowenantów lub ryzyko płynności, koduj go nie tylko jako `wyniki ekonomiczno-finansowe`, ale także jako potencjalny materiał dla `doświadczenie w zarządzaniu kryzysowym`, z ostrożnym ograniczeniem interpretacyjnym.

## Kontrola struktury przed zapisaniem

Przed oddaniem skoroszytu wykonaj następujące testy:

1. nagłówek `02_BAZA_DOWODOW` ma dokładnie 31 kolumn w określonej kolejności;
2. każdy wiersz ma dokładnie 31 pól;
3. `wyjatek_badacza` i `komentarz_badacza` są puste we wszystkich nowo utworzonych wierszach;
4. `propozycja_ai` zawiera wyłącznie dozwolone wartości;
5. `pytanie_do_wywiadu` zawiera pytanie albo jest puste — nigdy status, decyzję, propozycję AI ani komentarz;
6. każde ID jest unikalne, a wskazane ID konfliktów istnieją;
7. wszystkie adresy źródeł są przypisane do właściwych twierdzeń;
8. daty zdarzenia nie zostały mechanicznie zastąpione datami publikacji;
9. formuły, filtry, walidacje i arkusze pomocnicze nie zawierają błędów;
10. `status_przegladu_badacza = nierozpoczęty`.
11. `01_REJESTR_ZRODEL` zawiera zarówno źródła wykorzystane, jak i istotne ścieżki sprawdzone z wynikiem negatywnym;
12. każdy z siedmiu obszarów ma albo co najmniej jeden wiersz dowodowy, albo jawny wiersz `G. BRAK DANYCH`;
13. źródła aktualne lub późniejsze nie zostały przypisane mechanicznie do P3;
14. informacje o osobach, ofertach pracy, mediach społecznościowych i opiniach pracowniczych mają odnotowane ograniczenia datowania, reprezentatywności i poziomu analizy.
15. wykonano `Audyt domknięcia kwerendy` i zapisano go w `07_KONTROLA_JAKOSCI`;
16. każdy istotny trop z rejestru źródeł ma status: `potwierdzony`, `częściowo potwierdzony`, `niepotwierdzony`, `sprzeczny` albo `brak dostępu`;
17. czerwone flagi finansowe oraz tropy klientów/eksportu/systemów/ofert pracy zostały sprawdzone albo jawnie zapisane jako brak danych.

Dodaj listy rozwijane dla pól kontrolowanych, jeżeli format skoroszytu na to pozwala. Zablokuj przypadkowe zmiany nagłówków, ale nie blokuj pól przeznaczonych dla badacza.

## Wynik

Przekaż:

1. rejestr przejrzanych źródeł;
2. bazę dowodów w układzie jeden wiersz = jedno twierdzenie;
3. listę twierdzeń niezweryfikowanych;
4. listę konfliktów między źródłami;
5. listę braków informacyjnych;
6. pytania do wywiadów pogłębionych;
7. kontrolę jakości wskazującą liczbę wierszy, udział danych formalnych, narracji własnej firmy, informacji potwierdzonych niezależnie i informacji niezweryfikowanych oraz obszary o najwyższej i najniższej dostępności danych;
8. wynik audytu domknięcia kwerendy;
9. krótkie potwierdzenie wyniku testów strukturalnych.

Nie twórz syntetycznej oceny firmy ani końcowego raportu przypadku. Nie uruchamiaj automatycznie Promptu 2.
