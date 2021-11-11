ALTER /*+ NO_STATS */ TABLE "athlete" ADD PRIMARY KEY("code");
ALTER /*+ NO_STATS */ TABLE "event" ADD PRIMARY KEY("code");
ALTER /*+ NO_STATS */ TABLE "game" ADD PRIMARY KEY("host_year","event_code","athlete_code");
ALTER /*+ NO_STATS */ TABLE "history" ADD PRIMARY KEY("event_code","athlete");
ALTER /*+ NO_STATS */ TABLE "nation" ADD PRIMARY KEY("code");
ALTER /*+ NO_STATS */ TABLE "olympic" ADD PRIMARY KEY("host_year");
ALTER /*+ NO_STATS */ TABLE "participant" ADD PRIMARY KEY("host_year","nation_code");
ALTER /*+ NO_STATS */ TABLE "record" ADD PRIMARY KEY("host_year","event_code","athlete_code","medal");
ALTER /*+ NO_STATS */ TABLE "stadium" ADD PRIMARY KEY("code");
ALTER /*+ NO_STATS */ TABLE "test_hash_partition" ADD PRIMARY KEY("f1");
ALTER /*+ NO_STATS */ TABLE "test_list_partition" ADD PRIMARY KEY("f1");
ALTER /*+ NO_STATS */ TABLE "test_number" ADD PRIMARY KEY("id");
ALTER /*+ NO_STATS */ TABLE "test_range_partition" ADD PRIMARY KEY("f1");
ALTER /*+ NO_STATS */ TABLE "test_string" ADD PRIMARY KEY("id");
CREATE /*+ NO_STATS */ INDEX "idx_athlete_name" ON "athlete"("name");
CREATE /*+ NO_STATS */ INDEX "idx_game_game_date" ON "game"("game_date" DESC);
ALTER /*+ NO_STATS */ TABLE "game" ADD CONSTRAINT "fk_game_athlete_code" FOREIGN KEY ("athlete_code") REFERENCES "athlete"("code") ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER /*+ NO_STATS */ TABLE "game" ADD CONSTRAINT "fk_game_event_code" FOREIGN KEY ("event_code") REFERENCES "event"("code") ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER /*+ NO_STATS */ TABLE "participant" ADD CONSTRAINT "fk_participant_nation_code" FOREIGN KEY ("nation_code") REFERENCES "nation"("code") ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER /*+ NO_STATS */ TABLE "participant" ADD CONSTRAINT "fk_participant_host_year" FOREIGN KEY ("host_year") REFERENCES "olympic"("host_year") ON DELETE RESTRICT ON UPDATE RESTRICT;
UPDATE STATISTICS ON ALL CLASSES;
