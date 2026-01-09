/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import Popper from '@mui/material/Popper';
import { Box } from '@mui/material';

interface Project {
  projectId: string;
  displayName?: string;
}

interface ProjectSearchProps {
  projects: Project[];
  onSelect: (project: Project) => void;
  currentProject: string;
}

export const ProjectSearch: React.FC<ProjectSearchProps> = ({
  projects,
  onSelect,
  currentProject
}) => {
  const selectedValue =
    projects.find(p => p.projectId === currentProject) || null;

  return (
    <Box sx={{ minHeight: '60px', width: '100%', paddingBottom: '4px' }}>
      <Autocomplete
        options={projects}
        getOptionLabel={option => option.displayName || option.projectId}
        value={selectedValue}
        disablePortal={false}
        onChange={(_, newValue) => {
          if (newValue) onSelect(newValue);
        }}
        size="small"
        PopperComponent={props => (
          <Popper
            {...props}
            style={{ ...props.style, zIndex: 10000 }}
            placement="bottom-start"
          />
        )}
        renderInput={params => (
          <TextField
            {...params}
            label="Projects"
            variant="outlined"
            sx={{
              margin: '8px',
              width: 'calc(100% - 16px)',
              '& .MuiOutlinedInput-root': {
                backgroundColor: 'var(--jp-layout-color1)',
                color: 'var(--jp-ui-font-color1)',
                '& fieldset': { borderColor: 'var(--jp-border-color1)' },
                '&:hover fieldset': { borderColor: 'var(--jp-border-color2)' },
                '&.Mui-focused fieldset': {
                  borderColor: 'var(--jp-brand-color1)'
                }
              },
              '& .MuiInputLabel-root': {
                color: 'var(--jp-ui-font-color2)',
                '&.Mui-focused': { color: 'var(--jp-brand-color1)' }
              }
            }}
          />
        )}
      />
    </Box>
  );
};
