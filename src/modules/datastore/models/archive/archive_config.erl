%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on archive configuration.
%%% Archive configuration parameters are used to configure the process of
%%% creating an archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_config).
-author("Jakub Kudzia").

-include("modules/archive/archive.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([from_json/1, to_json/1, sanitize/1]).

%% Getters
-export([]).

-type config() :: #archive_config{}.
-type config_json() :: map().
%% #{
%%      <<"layout">> := layout(),
%%      <<"incremental">> => incremental(),
%%      <<"includeDip">> => include_dip(),
%% }
-type incremental() :: boolean().
-type include_dip() :: boolean().
-type layout() :: ?ARCHIVE_BAGIT_LAYOUT | ?ARCHIVE_PLAIN_LAYOUT.

-export_type([config/0, config_json/0, incremental/0, include_dip/0, layout/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(config_json()) -> config().
from_json(ConfigJson = #{
    <<"layout">> := Layout
}) ->
    #archive_config{

        layout = utils:to_atom(Layout),
        % optional values
        incremental = utils:to_boolean(maps:get(<<"incremental">>, ConfigJson, ?DEFAULT_INCREMENTAL)),
        include_dip = utils:to_boolean(maps:get(<<"includeDip">>, ConfigJson, ?DEFAULT_INCLUDE_DIP))
    }.


-spec to_json(config()) -> config_json().
to_json(#archive_config{
    incremental = Incremental,
    layout = Layout,
    include_dip = IncludeDip
}) ->
    #{
        <<"incremental">> => Incremental,
        <<"layout">> => str_utils:to_binary(Layout),
        <<"includeDip">> => IncludeDip
    }.


-spec sanitize(json_utils:json_map()) -> config_json().
sanitize(RawConfig) ->
    try
        middleware_sanitizer:sanitize_data(RawConfig, #{
            required => #{
                <<"layout">> => {atom, ?ARCHIVE_LAYOUTS}
            },
            optional => #{
                <<"includeDip">> => {boolean, any},
                <<"incremental">> => {boolean, any}
            }
        })
    catch
        % config is a nested object of the archive object,
        % therefore catch errors and add "config." to name of the key associated with the error
        throw:?ERROR_MISSING_REQUIRED_VALUE(Key) ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(str_utils:format_bin("config.~s", [Key])));
        throw:?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedVals) ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(str_utils:format_bin("config.~s", [Key]), AllowedVals));
        throw:?ERROR_BAD_VALUE_BOOLEAN(Key) ->
            throw(?ERROR_BAD_VALUE_BOOLEAN(str_utils:format_bin("config.~s", [Key])));
        throw:?ERROR_BAD_VALUE_ATOM(Key) ->
            throw(?ERROR_BAD_VALUE_ATOM(str_utils:format_bin("config.~s", [Key])))
    end.