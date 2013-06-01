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
-export([wrap_record/1, strip_wrappers/1]).

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


%% strip_wrappers/1
%% ====================================================================
%% @doc Strips #veil_document{} wrapper. Argument can be either an #veil_document{} or list of #veil_document{} <br/>
%% Alternatively arguments can be passed as {ok, Arg} tuple. Its convenient because most DAO methods formats return value formatted that way<br/>
%% If the argument cannot be converted (e.g. error tuple is passed), this method returns it unchanged. <br/>
%% This method is designed for use as wrapper for "get_*"-like DAO methods. E.g. dao_lib:strip_wrappers(dao_vfs:get_file({absolute_path, "/foo/bar"}))
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% @end
-spec strip_wrappers(VeilDocOrList :: #veil_document{} | [#veil_document{}]) -> tuple() | [tuple()].
%% ====================================================================
strip_wrappers({ok, List}) when is_list(List) ->
    strip_wrappers(List);
strip_wrappers(List) when is_list(List) ->
    [X || #veil_document{record = X} <- List];
strip_wrappers({ok, #veil_document{} = Doc}) ->
    strip_wrappers(Doc);
strip_wrappers(#veil_document{record = Record}) when is_tuple(Record) ->
    Record;
strip_wrappers(Other) ->
    Other.
    
%% ===================================================================
%% Internal functions
%% ===================================================================
    
