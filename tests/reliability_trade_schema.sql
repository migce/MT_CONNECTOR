-- Schema-only fixture captured 2026-09-05; no production rows.
CREATE TABLE public.trading_accounts (id integer PRIMARY KEY, enabled boolean NOT NULL DEFAULT true);
--
-- PostgreSQL database dump
--


-- Dumped from database version 16.11
-- Dumped by pg_dump version 16.11

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: trade_attempts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trade_attempts (
    id bigint NOT NULL,
    command_id uuid NOT NULL,
    attempt_no integer NOT NULL,
    phase text NOT NULL,
    retcode integer,
    message text,
    request_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    result_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: trade_attempts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.trade_attempts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: trade_attempts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.trade_attempts_id_seq OWNED BY public.trade_attempts.id;


--
-- Name: trade_commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trade_commands (
    id uuid NOT NULL,
    account_id integer NOT NULL,
    action text NOT NULL,
    status text DEFAULT 'accepted'::text NOT NULL,
    position_ticket bigint NOT NULL,
    expected_position_identifier bigint,
    expected_symbol text NOT NULL,
    expected_type integer NOT NULL,
    expected_magic bigint,
    max_volume double precision NOT NULL,
    reason text NOT NULL,
    correlation_id text,
    requested_by text NOT NULL,
    requested_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    claimed_at timestamp with time zone,
    submitted_at timestamp with time zone,
    completed_at timestamp with time zone,
    last_error text,
    result jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT trade_commands_action_check CHECK ((action = 'close_position'::text)),
    CONSTRAINT trade_commands_expected_type_check CHECK ((expected_type = ANY (ARRAY[0, 1]))),
    CONSTRAINT trade_commands_max_volume_check CHECK ((max_volume > (0)::double precision))
);


--
-- Name: trade_attempts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_attempts ALTER COLUMN id SET DEFAULT nextval('public.trade_attempts_id_seq'::regclass);


--
-- Name: trade_attempts trade_attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_attempts
    ADD CONSTRAINT trade_attempts_pkey PRIMARY KEY (id);


--
-- Name: trade_commands trade_commands_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_commands
    ADD CONSTRAINT trade_commands_pkey PRIMARY KEY (id);


--
-- Name: idx_trade_attempts_command; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trade_attempts_command ON public.trade_attempts USING btree (command_id, attempt_no, id);


--
-- Name: idx_trade_commands_account_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trade_commands_account_created ON public.trade_commands USING btree (account_id, created_at DESC);


--
-- Name: idx_trade_commands_dispatch; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trade_commands_dispatch ON public.trade_commands USING btree (status, next_attempt_at, created_at);


--
-- Name: uq_trade_commands_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_trade_commands_account_id ON public.trade_commands USING btree (account_id, id);


--
-- Name: trade_attempts trade_attempts_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_attempts
    ADD CONSTRAINT trade_attempts_command_id_fkey FOREIGN KEY (command_id) REFERENCES public.trade_commands(id) ON DELETE CASCADE;


--
-- Name: trade_commands trade_commands_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trade_commands
    ADD CONSTRAINT trade_commands_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.trading_accounts(id);


--
-- PostgreSQL database dump complete
--



SET search_path TO public;
