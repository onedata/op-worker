%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file defines modules used by system and their initial arguments.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(MODULES_AND_ARGS_HRL).
-define(MODULES_AND_ARGS_HRL, 1).

-define(MODULES, [
    datastore_worker,
    http_worker,
    dns_worker,
    event_manager_worker,
    sequencer_worker
]).

-define(MODULES_WITH_ARGS, [
    {datastore_worker, []},
    {http_worker, []},
    {dns_worker, []},
    {event_manager_worker, []},
    {sequencer_worker, [
        {supervisor_spec, sequencer_worker:supervisor_spec()},
        {supervisor_children_spec, sequencer_worker:supervisor_children_spec()}
    ]}
]).

-endif.