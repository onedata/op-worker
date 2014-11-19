%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common macros and records for RTransfer
%% container.
%% @end
%% ===================================================================

-ifndef(RT_CONTAINER_HRL).
-define(RT_CONTAINER_HRL, 1).

-export_type([container_name/0, container_ref/0, container_ptr/0]).

-type container_name() :: {local, Name :: atom()} |
{global, GlobalName :: term()} |
{via, Module :: module(), ViaName :: term()}.

-type container_ref() :: atom() | pid() |
{Name :: atom(), Node :: node()} |
{global, GlobalName :: term()} |
{via, Module :: module(), ViaName :: term()}.

-type container_ptr() :: term().

%% gen_server state
%% * container - pointer to container resource created as a call to rt_container:init_nif() function
%% * type - type of container (priority_queue | map)
%% * size - amount of elements stored in the container
-record(state, {container, type, size = 0}).

%% RTransfer container element
-record(rt_block, {file_id = "", provider_id = <<>>, offset = 0, size = 0, priority = 0, pids = []}).

-endif.