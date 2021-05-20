%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO VFS-7674 Describe automation workflow execution machinery
%%% @end
%%%--------------------------------------------------------------------
-module(atm_execution).
-author("Bartosz Walkowicz").

-type item() :: json_utils:json_term().

-export_type([item/0]).

%%%===================================================================
%%% API
%%%===================================================================
