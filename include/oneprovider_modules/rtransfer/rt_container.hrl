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

%% RTransfer container element
-record(rt_block, {
    file_id = "" :: string(),
    provider_ref :: term(),
    offset = 0 :: non_neg_integer(),
    size = 1 :: pos_integer(),
    priority = 0 :: non_neg_integer(),
    retry = 0 :: non_neg_integer(),
    terms = [] :: [term()]
}).

-endif.
