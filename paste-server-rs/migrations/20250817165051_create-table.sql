-- Add migration script here
CREATE TABLE IF NOT EXISTS paste (
id UUID PRIMARY KEY,
title TEXT NOT NULL,
expiration timestamp NOT NULL,
language TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS attachments (
id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
filename text NOT NULL,
paste_id UUID NOT NULL
);
