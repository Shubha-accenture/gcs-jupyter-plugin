/**
 * @license
 * Copyright 2025 Google LLC
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

import React, { ReactElement, JSXElementConstructor } from 'react';
import { ReactWidget } from '@jupyterlab/apputils';

const IndeterminateProgressBarComponent: React.FC = () => {
  return (
    <div
      style={{
        height: '1px',
        backgroundColor: 'rgba(5, 114, 206, 0.2)',
        width: '100%',
        overflow: 'hidden',
        position: 'relative',
        display: 'block',
        visibility: 'visible',
        opacity: 1,
        border: 'none',
        boxShadow: 'none',
        margin: 0,
        padding: 0
      }}
    >
      <div
        style={{
          height: '100%',
          width: '100%',
          backgroundColor: 'rgb(25, 118, 210)',
          animation: 'customIndeterminateAnimation 1s infinite linear',
          transformOrigin: '0% 50%',
          display: 'block',
          visibility: 'visible',
          opacity: 1,
          margin: 0,
          padding: 0
        }}
      ></div>
      <style>{`
        @keyframes customIndeterminateAnimation {
          0% {
            transform: translateX(0) scaleX(0);
          }
          40% {
            transform: translateX(0) scaleX(0.4);
          }
          100% {
            transform: translateX(100%) scaleX(0.5);
          }
        }
      `}</style>
    </div>
  );
};

export class ProgressBarWidget extends ReactWidget {
  constructor() {
    super();
    this.node.style.flexShrink = '0';
    this.node.style.display = 'none'; // Initially hidden
  }

  protected render(): ReactElement<any, string | JSXElementConstructor<any>> {
    return <IndeterminateProgressBarComponent />;
  }

  public show(): void {
    this.node.classList.remove('lm-mod-hidden');
    this.node.style.display = 'flex';
  }

  public hide(): void {
    this.node.classList.add('lm-mod-hidden');
    this.node.style.display = 'none';
  }
}
