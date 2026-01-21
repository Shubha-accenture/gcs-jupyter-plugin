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
import { Box, CircularProgress } from '@mui/material';
import { ProjectSearchProps } from '../interfaces/ProjectInterfaces';

export const ProjectSearch: React.FC<ProjectSearchProps> = ({
  projects,
  onSelect,
  currentProject,
  disabled
}) => {
  const selectedValue =
    projects.find(p => p.projectId === currentProject) || null;

  return (
    <Box sx={{ minHeight: '40px', width: '100%', paddingTop: '4px' }}>
      <Autocomplete
        options={projects}
        getOptionLabel={option => option.displayName || option.projectId}
        value={selectedValue}
        disablePortal={false}
        disabled={disabled}
        onChange={(_, newValue) => {
          if (newValue) onSelect(newValue);
        }}
        size="small"
        slotProps={{
          listbox: {
            sx: {
              fontSize: '13px', // Font size for the list items
              '& .MuiAutocomplete-option': {
                minHeight: '30px', //match the height of your input
                padding: '4px 10px'
              }
            }
          },
          popper: { sx: { zIndex: 10000 }, placement: 'bottom-start' }
        }}
        renderInput={params => (
          <TextField
            {...params}
            label="Project"
            variant="outlined"
            InputProps={{
              ...params.InputProps,
              endAdornment: (
                <React.Fragment>
                  {disabled ? (
                    <CircularProgress color="inherit" size={20} />
                  ) : null}
                  {params.InputProps.endAdornment}
                </React.Fragment>
              )
            }}
            sx={{
              margin: '8px',
              width: 'calc(100% - 16px)',
              '& .MuiOutlinedInput-root': {
                height: '30px',
                fontSize: '13px',
                borderRadius: '2px',
                backgroundColor: 'var(--jp-layout-color1)',
                color: 'var(--jp-ui-font-color1)',
                paddingTop: '0px',
                paddingBottom: '0px',
                '& fieldset': { borderColor: '#707070' },
                '&:hover fieldset': { borderColor: '#707070' },
                '&.Mui-focused fieldset': {
                  borderColor: 'var(--jp-brand-color1)'
                }
              },
              '& .MuiInputLabel-root': {
                fontSize: '13px',
                color: 'var(--jp-ui-font-color2)',
                top: '-2px',
                '&.Mui-focused': { color: 'var(--jp-brand-color1)' }
              },
              '& .MuiInputLabel-shrink': {
                top: '0px'
              }
            }}
          />
        )}
      />
    </Box>
  );
};
