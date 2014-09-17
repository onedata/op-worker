%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing basic operations on
%% cdmi objects
%% @end
%% ===================================================================
-module(cdmi_objectid).

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_capabilities.hrl").

%% API
-export([allowed_methods/2, malformed_request/2]).

%% allowed_methods/2
%% ====================================================================
%% @doc
%% Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
%% @end
%% ====================================================================
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    case is_capability_object(Req) of
        true -> {[<<"GET">>], Req, State};
        false -> {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}
    end.

%% malformed_request/2
%% ====================================================================
%% @doc
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
%% ====================================================================
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}} | no_return().
%% ====================================================================
malformed_request(Req, State = #state{filepath = Filepath}) ->
    % get objectid
    {PathInfo, Req2} = cowboy_req:path_info(Req),
    [<<"cdmi_objectid">>, Id | _Rest] = PathInfo,

    % get uuid from objectid
    Uuid = case cdmi_id:objectid_to_uuid(Id) of
               Uuid_ when is_list(Uuid_) -> Uuid_;
               _ -> throw(cdmi_error:error_reply(Req2, State, {invalid_objectid, Id}))
           end,

    % get path of object with that uuid
    BaseName = case is_capability_object(Req2) of
                   true -> proplists:get_value(Id,?CapabilityPathById);
                   false ->
                       case logical_files_manager:get_file_full_name_by_uuid(Uuid) of
                           {ok, Name} when is_list(Name) ->Name,
                               Path = string:tokens(fslogic_path:get_short_file_name(Name),"/"),
                               case Path of
                                   [] -> "/";
                                   List -> filename:join(List)
                               end;
                           _ -> throw(cdmi_error:error_reply(Req2, State, not_found))
                       end
               end,

    % join base name with the rest of filepath
    ["cdmi_objectid", _Id | Rest] = string:tokens(Filepath, "/"),
    FullFileName = filename:join([BaseName | Rest]),

    % delegate request to cdmi_container/cdmi_object/cdmi_capability (depending on filepath)
    {Url, Req3} = cowboy_req:path(Req2),
    case binary:last(Url) =:= $/ of
        true ->
            case is_capability_object(Req3) of
                false -> cdmi_container:malformed_request(Req3,State#state{filepath = FullFileName, handler_module = cdmi_container});
                true -> cdmi_capabilities:malformed_request(Req3,State#state{filepath = FullFileName, handler_module = cdmi_capabilities})
            end;
        false -> cdmi_object:malformed_request(Req3,State#state{filepath = FullFileName, handler_module = cdmi_object})
    end.

%% ====================================================================
%% From now on, as we are sure that object with given id exists, we delegate all subsequent calls to
%% adequate cdmi_modules (cdmi_container/cdmi_object) by changing handler in request state. The normal
%% cowboy flow continues in cdmi_container/cdmi_object.
%% ====================================================================

%% ====================================================================
%% Internal functions
%% ====================================================================

%% is_capability_object/1
%% ====================================================================
%% @doc Checks if this objectid request points to capability object
%% ====================================================================
-spec is_capability_object(req()) -> boolean().
%% ====================================================================
is_capability_object(Req) ->
    {PathInfo, _} = cowboy_req:path_info(Req),
    case PathInfo of
        [<<"cdmi_objectid">>, Id | _Rest] ->
            proplists:is_defined(Id,?CapabilityPathById);
        _ -> false
    end.
