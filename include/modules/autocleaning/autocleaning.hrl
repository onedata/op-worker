%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common macros used in modules associated with autocleaning mechanism.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(AUTOCLEANING_HRL).
-define(AUTOCLEANING_HRL, 1).

% Macros defining possible states of an autocleaning run.
-define(ACTIVE, active).
-define(CANCELLING, cancelling).
-define(COMPLETED, completed).
-define(FAILED, failed).
-define(CANCELLED, cancelled).

-endif.