/*
 *  Copyright (C) 2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.util.logging.Level;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class AllowEventListener extends FilteredEventListener {
    static EventListener make(EventListener listener, EventType.Category... categories) {
        final EventListener original = listener;

        Set<EventType.Category> categorySet;
        if (categories.length == 0) {
            categorySet = Collections.emptySet();
        } else {
            categorySet = new HashSet<>(Arrays.asList(categories));
        }

        Set<Level> levelSet;
        if (listener instanceof AllowEventListener) {
            AllowEventListener allowed = (AllowEventListener) listener;
            listener = allowed.mListener;
            categorySet.retainAll(allowed.mCategories);
            levelSet = allowed.mLevels;
        } else {
            levelSet = null;
        }

        return make(original, listener, categorySet, levelSet);
    }

    static EventListener make(EventListener listener, Level... levels) {
        final EventListener original = listener;

        Set<Level> levelSet;
        if (levels.length == 0) {
            levelSet = Collections.emptySet();
        } else {
            levelSet = new HashSet<>(Arrays.asList(levels));
        }

        Set<EventType.Category> categorySet;
        if (listener instanceof AllowEventListener) {
            AllowEventListener allowed = (AllowEventListener) listener;
            listener = allowed.mListener;
            levelSet.retainAll(allowed.mLevels);
            categorySet = allowed.mCategories;
        } else {
            categorySet = null;
        }

        return make(original, listener, categorySet, levelSet);
    }

    private static EventListener make(EventListener original, EventListener listener,
                                      Set<EventType.Category> categories, Set<Level> levels)
    {
        EventListener newListener = new AllowEventListener(listener, categories, levels);
        return newListener.equals(original) ? original : newListener;
    }

    AllowEventListener(EventListener listener,
                       Set<EventType.Category> categories, Set<Level> levels)
    {
        super(listener, categories, levels);
    }

    @Override
    public boolean isObserved(EventType.Category category) {
        return mCategories == null || mCategories.contains(category);
    }

    @Override
    public boolean isObserved(Level level) {
        return mLevels == null || mLevels.contains(level);
    }

    @Override
    protected boolean sameClass(Object obj) {
        return obj.getClass() == AllowEventListener.class;
    }
}
