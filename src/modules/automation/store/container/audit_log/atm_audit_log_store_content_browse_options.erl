%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse options specialization for
%%% audit_log store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_content_browse_options).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_options).

-include("modules/automation/atm_execution.hrl").

%% API
-export([sanitize/1]).


-type record() :: #atm_audit_log_store_content_browse_options{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record() | no_return().
sanitize(#{<<"type">> := <<"auditLogStoreContentBrowseOptions">>} = Data) ->
    #atm_audit_log_store_content_browse_options{
        browse_opts = audit_log_browse_opts:sanitize(Data)
    }.
