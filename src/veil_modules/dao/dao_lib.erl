%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: DAO helper/utility functional methods. Those can be used in other modules
%% bypassing worker_host and gen_server.
%% @end
%% ===================================================================
-module(dao_lib).

-include_lib("veil_modules/dao/dao.hrl").

%% API
-export([wrap_record/1, strip_wrapper/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% wrap_record/1
%% ====================================================================
%% @doc Wraps given erlang record with #veil_document{} wrapper. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% @end
-spec wrap_record(Record :: tuple()) -> #veil_document{}.
%% ====================================================================
wrap_record(Record) when is_tuple(Record) ->
    #veil_document{record = Record}.


%% strip_wrapper/1
%% ====================================================================
%% @doc Strips #veil_document{} wrapper. Argument can be either an #veil_document{} or list of #veil_document{} <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% @end
-spec strip_wrapper(VeilDocOrList :: #veil_document{} | [#veil_document{}]) -> tuple() | [tuple()].
%% ====================================================================
strip_wrapper({ok, List}) when is_list(List) ->
    strip_wrapper(List);
strip_wrapper(List) when is_list(List) ->
    [X || #veil_document{record = X} <- List];
strip_wrapper(#veil_document{record = Record}) when is_tuple(Record) ->
    Record.
    
%% ===================================================================
%% Internal functions
%% ===================================================================
    
